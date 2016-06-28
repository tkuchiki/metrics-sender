package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/BurntSushi/toml"
	_ "github.com/go-sql-driver/mysql"
	"regexp"
	"strconv"
	"time"
)

type MySQL struct {
	db     *sql.DB
	config MySQLConfig
	log    Logger
}

type MySQLConfig struct {
	Host     string                          `toml:"host"`
	User     string                          `toml:"user"`
	Port     int                             `toml:"port"`
	Password string                          `toml:"password"`
	Timeout  int                             `toml:"timeout"`
	Timezone string                          `toml:"timezone"`
	Metrics  map[string][]MySQLMetricsConfig `toml:"metrics"`
}

type MySQLMetricsConfig struct {
	MetricsConfig
}

func NewMySQL(mysqlConfig MySQLConfig, filename string, log Logger) (Input, error) {
	var err error
	var mysql *MySQL
	var str string
	str, err = LoadFile(filename)

	if err != nil {
		return mysql, err
	}

	var config MySQLConfig
	_, err = toml.Decode(str, &config)

	if err != nil {
		return mysql, err
	}

	if mysqlConfig.Host != "" {
		config.Host = mysqlConfig.Host
	}

	if mysqlConfig.User != "" {
		config.User = mysqlConfig.User
	}

	if mysqlConfig.Port != 0 {
		config.Port = mysqlConfig.Port
	} else if config.Port == 0 {
		config.Port = 3306
	}

	if mysqlConfig.Password != "" {
		config.Password = mysqlConfig.Password
	}

	if mysqlConfig.Timeout > 0 {
		config.Timeout = mysqlConfig.Timeout
	}

	if mysqlConfig.Timezone != "" {
		config.Timezone = mysqlConfig.Timezone
	}

	var db *sql.DB
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=%ds&readTimeout=%ds", config.User, config.Password,
		config.Host, config.Port, config.Timeout, config.Timeout)
	db, err = sql.Open("mysql", dsn)

	mysql = &MySQL{
		db:     db,
		config: config,
		log:    log,
	}

	return mysql, err
}

func (m *MySQL) showGlobalStatus() (map[string]float64, error) {
	var err error
	var stats map[string]float64

	rows, err := m.db.Query("SHOW GLOBAL STATUS")

	if err != nil {
		return stats, err
	}

	stats = make(map[string]float64)
	for rows.Next() {
		var value string
		var name string
		var fval float64
		if err = rows.Scan(&name, &value); err == nil {
			fval, err = strconv.ParseFloat(string(value), 64)
			if err != nil {
				continue
			}

			stats[name] = fval
		}
	}

	return stats, err
}

func (m *MySQL) showSlaveStatus() (map[string]float64, error) {
	var err error
	var stats map[string]float64

	rows, err := m.db.Query("SHOW SLAVE STATUS")

	if err != nil {
		return stats, err
	}

	stats, err = m.fetchSlaveStatus(rows)

	return stats, err
}

func (m *MySQL) showEngineInnodbStatus() (map[string]float64, error) {
	var t string // type
	var n string // name
	var status string
	var data map[string]float64

	err := m.db.QueryRow("SHOW ENGINE INNODB STATUS").Scan(&t, &n, &status)

	if err != nil {
		return data, err
	}

	data = make(map[string]float64)

	//BACKGROUND THREAD
	srvLoops := findStringSubMatch(`(?m)srv_master_thread loops: (\d+) srv_active, (\d+) srv_shutdown, (\d+) srv_idle`, status, 1, 2, 3)
	if len(srvLoops) > 0 {
		data["Master_thread_loops_srv_active"] = srvLoops[0]
		data["Master_thread_loops_srv_shutdowne"] = srvLoops[1]
		data["Master_thread_loops_srv_idle"] = srvLoops[2]
	}

	srvLog := findStringSubMatch(`(?m)srv_master_thread log flush and writes: (\d+)`, status, 1)
	if len(srvLog) > 0 {
		data["Master_thread_log_flush_and_writes"] = srvLog[0]
	}

	//SEMAPHORES
	osReservationCnt := findStringSubMatch(`(?m)OS WAIT ARRAY INFO: reservation count (\d+)`, status, 1)
	if len(osReservationCnt) > 0 {
		data["Os_wait_array_reservation_count"] = osReservationCnt[0]
	}
	osSignalCnt := findStringSubMatch(`(?m)OS WAIT ARRAY INFO: signal count (\d+)`, status, 1)
	if len(osSignalCnt) > 0 {
		data["Os_wait_array_signal_count"] = osSignalCnt[0]
	}
	mutexSpins := findStringSubMatch(`(?m)Mutex spin waits (\d+), rounds (\d+), OS waits (\d+)`, status, 1, 2, 3)
	if len(mutexSpins) > 0 {
		data["Mutex_spin_waits"] = mutexSpins[0]
		data["Mutex_spin_rounds"] = mutexSpins[1]
		data["Mutex_spin_os_waits"] = mutexSpins[2]
	}
	rwSpins := findStringSubMatch(`(?m)RW-shared spins (\d+), rounds (\d+), OS waits (\d+)`, status, 1, 2, 3)
	if len(rwSpins) > 0 {
		data["Rw_shared_spins"] = rwSpins[0]
		data["Rw_shared_rounds"] = rwSpins[1]
		data["Rw_shared_os_waits"] = rwSpins[2]
	}
	rwExcl := findStringSubMatch(`(?m)RW-excl spins (\d+), rounds (\d+), OS waits (\d+)`, status, 1, 2, 3)
	if len(rwExcl) > 0 {
		data["Rw_excl_spins"] = rwExcl[0]
		data["Rw_excl_rounds"] = rwExcl[1]
		data["Rw_excl_os_waits"] = rwExcl[2]
	}
	spinRounds := findStringSubMatch(`Spin rounds per wait: (\d+(\.\d+)?) mutex, (\d+(\.\d+)?) RW-shared, (\d+(\.\d+)?) RW-excl`, status, 1, 3, 5)
	if len(spinRounds) > 0 {
		data["Spin_rounds_mutex"] = spinRounds[0]
		data["Spin_rounds_rw_shared"] = spinRounds[1]
		data["Spin_rounds_rw_excl"] = spinRounds[2]
	}

	// FILE I/O
	pendingAio := findStringSubMatch(`(?m)Pending normal aio reads: (\d+) \[.+\] , aio writes: (\d+) \[.+\] ,\n\s+ibuf aio reads: (\d+), log i/o's: (\d+), sync i/o's: (\d+)`, status, 1, 2, 3, 4, 5)
	if len(pendingAio) > 0 {
		data["Pending_aio_reads"] = pendingAio[0]
		data["Pending_aio_writes"] = pendingAio[1]
		data["Pending_ibuf_aio_reads"] = pendingAio[2]
		data["Pending_ibuf_aio_log"] = pendingAio[3]
		data["Pending_ibuf_aio_sync"] = pendingAio[4]
	}
	pendingFlushes := findStringSubMatch(`(?m)Pending flushes \(fsync\) log: (\d+); buffer pool: (\d+)`, status, 1, 2)
	if len(pendingFlushes) > 0 {
		data["Pending_flushes_fsync_log"] = pendingFlushes[0]
		data["Pending_flushes_buffer_pool"] = pendingFlushes[1]
	}
	osFiles := findStringSubMatch(`(?m)(\d+) OS file reads, (\d+) OS file writes, (\d+) OS fsyncs`, status, 1, 2, 3)
	if len(osFiles) > 0 {
		data["Os_file_reads"] = osFiles[0]
		data["Os_file_writes"] = osFiles[1]
		data["Os_file_fsyncs"] = osFiles[2]
	}
	files := findStringSubMatch(`(?m)(\d+(\.\d+)?) reads/s, (\d+(\.\d+)?) avg bytes/read, (\d+(\.\d+)?) writes/s, (\d+(\.\d+)?) fsyncs/s`, status, 1, 3, 5, 7)
	if len(files) > 0 {
		data["File_reads_per_secs"] = files[0]
		data["File_avg_bytes_per_read"] = files[1]
		data["File_writes_per_secs"] = files[2]
		data["File_fsyncs_per_secs"] = files[3]
	}

	//INSERT BUFFER AND ADAPTIVE HASH INDEX
	ibuf := findStringSubMatch(`(?m)Ibuf: size (\d+), free list len (\d+), seg size (\d+), (\d+) merges`, status, 1, 2, 3, 4)
	if len(ibuf) > 0 {
		data["Ibuf_table_space"] = ibuf[0]
		data["Ibuf_free_list_len"] = ibuf[1]
		data["Ibuf_seg"] = ibuf[2]
		data["Ibuf_merges"] = ibuf[3]
	}
	ibufMerged := findStringSubMatch(`(?m)merged operations:\n\s+insert (\d+), delete mark (\d+), delete (\d+)`, status, 1, 2, 3)
	if len(ibufMerged) > 0 {
		data["Ibuf_merged_insert"] = ibufMerged[0]
		data["Ibuf_merged_delete_mark"] = ibufMerged[1]
		data["Ibuf_merged_delete"] = ibufMerged[2]
	}
	ibufDiscarded := findStringSubMatch(`(?m)discarded operations:\n\s+insert (\d+), delete mark (\d+), delete (\d+)`, status, 1, 2, 3)
	if len(ibufDiscarded) > 0 {
		data["Ibuf_discarded_insert"] = ibufDiscarded[0]
		data["Ibuf_discarded_delete_mark"] = ibufDiscarded[1]
		data["Ibuf_discarded_delete"] = ibufDiscarded[2]
	}
	hashTable := findStringSubMatch(`(?m)Hash table size (\d+), node heap has (\d+) buffer\(s\)`, status, 1, 2)
	if len(hashTable) > 0 {
		data["Hash_table_size"] = hashTable[0]
		data["Hash_table_heep_buffer"] = hashTable[1]
	}
	hashSearches := findStringSubMatch(`(?m)(\d+(\.\d+)?) hash searches/s, (\d+(\.\d+)?) non-hash searches/s`, status, 1, 3)
	if len(hashSearches) > 0 {
		data["Hash_searches_per_sec"] = hashSearches[0]
		data["Btree_searches_per_sec"] = hashSearches[1]
	}

	//LOG
	logSequence := findStringSubMatch(`(?m)Log sequence number\s+(\d+)`, status, 1)
	if len(logSequence) > 0 {
		data["Log_sequence_number"] = logSequence[0]
	}
	logFlushed := findStringSubMatch(`(?m)Log flushed up to\s+(\d+)`, status, 1)
	if len(logFlushed) > 0 {
		data["Log_flushed"] = logFlushed[0]
	}
	logPagesFlushed := findStringSubMatch(`(?m)Pages flushed up to\s+(\d+)`, status, 1)
	if len(logPagesFlushed) > 0 {
		data["Log_pages_flushed"] = logPagesFlushed[0]
	}
	lastCheckpoint := findStringSubMatch(`(?m)Last checkpoint at\s+(\d+)`, status, 1)
	if len(lastCheckpoint) > 0 {
		data["Last_checkpoint"] = lastCheckpoint[0]
	}
	logPendingWrites := findStringSubMatch(`(?m)(\d+) pending log writes, (\d+) pending chkp writes`, status, 1, 2)
	if len(logPendingWrites) > 0 {
		data["Pending_log_writes"] = logPendingWrites[0]
		data["Pending_chkp_writes"] = logPendingWrites[1]
	}
	logIO := findStringSubMatch(`(?m)(\d+) log i/o's done, (\d+(\.\d+)?) log i/o's/second`, status, 1, 2)
	if len(logIO) > 0 {
		data["Log_io_done"] = logIO[0]
		data["Log_io_second"] = logIO[1]
	}

	//BUFFER POOL AND MEMORY
	memoryAllocated := findStringSubMatch(`(?m)Total memory allocated (\d+); in additional pool allocated (\d+)`, status, 1, 2)
	if len(memoryAllocated) > 0 {
		data["Total_memory_allocated"] = memoryAllocated[0]
		data["Additional_pool_allocated"] = memoryAllocated[1]
	}
	dictAllocated := findStringSubMatch(`(?m)Dictionary memory allocated\s+(\d+)`, status, 1)
	if len(dictAllocated) > 0 {
		data["Dictionary_memory_allocated"] = dictAllocated[0]
	}
	bufferPool := findStringSubMatch(`(?m)Buffer pool size\s+(\d+)`, status, 1)
	if len(bufferPool) > 0 {
		data["Buffer_pool_size"] = bufferPool[0]
	}
	freeBuffers := findStringSubMatch(`(?m)Free buffers\s+(\d+)`, status, 1)
	if len(freeBuffers) > 0 {
		data["Free_buffers"] = freeBuffers[0]
	}
	dbPages := findStringSubMatch(`(?m)Database pages\s+(\d+)`, status, 1)
	if len(dbPages) > 0 {
		data["Database_pages"] = dbPages[0]
	}
	oldDbPages := findStringSubMatch(`(?m)Old database pages\s+(\d+)`, status, 1)
	if len(oldDbPages) > 0 {
		data["Old_database_pages"] = oldDbPages[0]
	}
	modifiedDbPages := findStringSubMatch(`(?m)Modified db pages\s+(\d+)`, status, 1)
	if len(modifiedDbPages) > 0 {
		data["Modified_db_pages"] = modifiedDbPages[0]
	}
	pendingReads := findStringSubMatch(`(?m)Pending reads (\d+)`, status, 1)
	if len(pendingReads) > 0 {
		data["Pending_reads"] = pendingReads[0]
	}
	pendingWrites := findStringSubMatch(`(?m)Pending writes: LRU (\d+), flush list (\d+), single page (\d+)`, status, 1, 2, 3)
	if len(pendingWrites) > 0 {
		data["Pending_writes_lru"] = pendingWrites[0]
		data["Pending_writes_flush_list"] = pendingWrites[1]
		data["Pending_writes_single_page"] = pendingWrites[2]
	}
	pagesMadeYoung := findStringSubMatch(`(?m)Pages made young (\d+), not young (\d+)`, status, 1, 2)
	if len(pagesMadeYoung) > 0 {
		data["Pages_made_young"] = pagesMadeYoung[0]
		data["Pages_made_not_young"] = pagesMadeYoung[1]
	}
	pagesMadeYoungSecs := findStringSubMatch(`(?m)(\d+(\.\d+)?) youngs/s, (\d+(\.\d+)?) non-youngs/s`, status, 1, 3)
	if len(pagesMadeYoungSecs) > 0 {
		data["Pages_made_young_per_secs"] = pagesMadeYoungSecs[0]
		data["Pages_made_not_young_per_secs"] = pagesMadeYoungSecs[1]
	}
	pagesOperations := findStringSubMatch(`(?m)Pages read (\d+), created (\d+), written (\d+)`, status, 1, 2, 3)
	if len(pagesOperations) > 0 {
		data["Pages_read"] = pagesOperations[0]
		data["Pages_created"] = pagesOperations[1]
		data["Pages_written"] = pagesOperations[2]
	}
	pagesOperationsSecs := findStringSubMatch(`(?m)(\d+(\.\d+)?) reads/s, (\d+(\.\d+)?) creates/s, (\d+(\.\d+)?) writes/s`, status, 1, 3, 5)
	if len(pagesOperationsSecs) > 0 {
		data["Pages_reads_per_secs"] = pagesOperationsSecs[0]
		data["Pages_creates_per_secs"] = pagesOperationsSecs[1]
		data["Pages_writes_per_secs"] = pagesOperationsSecs[2]
	}
	bufferPoolHitRate := findStringSubMatch(`(?m)Buffer pool hit rate (\d+) / (\d+), young-making rate (\d+) / (\d+) not (\d+) / (\d+)`, status, 1, 2, 3, 4, 5, 6)
	if len(bufferPoolHitRate) > 0 {
		data["Buffer_pool_hit_rate"] = bufferPoolHitRate[0] / bufferPoolHitRate[1]
		data["Buffer_pool_young_making_rate"] = bufferPoolHitRate[2] / bufferPoolHitRate[3]
		data["Buffer_pool_miss_rate"] = bufferPoolHitRate[4] / bufferPoolHitRate[5]
	}
	pagesInfo := findStringSubMatch(`(?m)Pages read ahead (\d+(\.\d+)?)/s, evicted without access (\d+(\.\d+)?)/s, Random read ahead (\d+(\.\d+)?)/s`, status, 1, 3, 5)
	if len(pagesInfo) > 0 {
		data["Pages_read_ahead_per_secs"] = pagesInfo[0]
		data["Pages_evicted_without_access_per_secs"] = pagesInfo[1]
		data["Pages_random_read_ahead_per_secs"] = pagesInfo[2]
	}
	lru := findStringSubMatch(`(?m)LRU len: (\d+), unzip_LRU len: (\d+)`, status, 1, 2)
	if len(lru) > 0 {
		data["LRU_len"] = lru[0]
		data["LRU_unzip_len"] = lru[1]
	}
	bufferPoolIO := findStringSubMatch(`(?m)I/O sum\[(\d+)\]:cur\[(\d+)\], unzip sum\[(\d+)\]:cur\[(\d+)\]`, status, 1, 2, 3, 4)
	if len(bufferPoolIO) > 0 {
		data["Buffer_pool_io_sum"] = bufferPoolIO[0]
		data["Buffer_pool_io_cur"] = bufferPoolIO[1]
		data["Buffer_pool_io_unzip_sum"] = bufferPoolIO[2]
		data["Buffer_pool_io_unzip_cur"] = bufferPoolIO[3]
	}

	//ROW OPERATIONS
	queries := findStringSubMatch(`(?m)(\d+) queries inside InnoDB, (\d+) queries in queue`, status, 1, 2)
	if len(queries) > 0 {
		data["Queries_inside_innodb"] = queries[0]
		data["Queries_in_queue"] = queries[1]
	}
	readViewsOpen := findStringSubMatch(`(?m)(\d+) read views open inside InnoDB`, status, 1)
	if len(readViewsOpen) > 0 {
		data["Read_views_open"] = readViewsOpen[0]
	}
	numRows := findStringSubMatch(`(?m)Number of rows inserted (\d+), updated (\d+), deleted (\d+), read (\d+)`, status, 1, 2, 3, 4)
	if len(numRows) > 0 {
		data["Innodb_row_inserts"] = numRows[0]
		data["Innodb_row_updates"] = numRows[1]
		data["Innodb_row_deletes"] = numRows[2]
		data["Innodb_row_reads"] = numRows[3]
	}
	numRowsSecs := findStringSubMatch(`(?m)(\d+(\.\d+)?) inserts/s, (\d+(\.\d+)?) updates/s, (\d+(\.\d+)?) deletes/s, (\d+(\.\d+)?) reads/s`, status, 1, 3, 5, 7)
	if len(numRowsSecs) > 0 {
		data["Innodb_row_inserts_per_secs"] = numRows[0]
		data["Innodb_row_updates_per_secs"] = numRows[1]
		data["Innodb_row_deletes_per_secs"] = numRows[2]
		data["Innodb_row_reads_per_secs"] = numRows[3]
	}

	return data, err
}

func findStringSubMatch(s, target string, index ...int) []float64 {
	var err error
	var data []float64
	re := regexp.MustCompile(s)
	group := re.FindStringSubmatch(target)

	if len(group) == 0 {
		return data
	}

	data = make([]float64, 0, len(index))
	var fval float64
	for _, i := range index {
		fval, err = strconv.ParseFloat(string(group[i]), 64)
		if err != nil {
			continue
		}
		data = append(data, fval)
	}

	return data
}

func (m *MySQL) fetchRows(rows *sql.Rows) (map[string]float64, error) {
	data := make(map[string]float64)
	columns, err := rows.Columns()
	if err != nil {
		return data, err
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	if !rows.Next() {
		return data, errors.New("empty rows")
	}

	err = rows.Scan(scanArgs...)
	if err != nil {
		return data, err
	}

	var fval float64
	for i, val := range values {
		fval, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			continue
		}

		data[columns[i]] = fval
	}

	return data, err
}

func (m *MySQL) fetchSlaveStatus(rows *sql.Rows) (map[string]float64, error) {
	data := make(map[string]float64)

	tmpData, err := m.fetchRows(rows)

	if err != nil {
		return data, err
	}

	data["Seconds_Behind_Master"] = tmpData["Seconds_Behind_Master"]
	data["Master_Log_Pos"] = tmpData["Read_Master_Log_Pos"] - tmpData["Exec_Master_Log_Pos"]
	data["Relay_Log_Space"] = tmpData["Relay_Log_Space"]

	return data, err
}

func setMetrics(metrics *[]Metric, config []MySQLMetricsConfig, stats map[string]float64, now time.Time) {
	for _, c := range config {
		names := c.SplitName()
		for _, n := range names {
			name := c.CreateName(n)
			value := c.CalcValue(stats[c.Name])

			*metrics = append(*metrics, Metric{
				Name:  name,
				Value: value,
				Time:  now,
			})
		}
	}
}

func (m *MySQL) FetchMetrics() ([]Metric, error) {
	var err error
	var now time.Time

	globalStatusLen := len(m.config.Metrics["global_status"])
	innodbStatusLen := len(m.config.Metrics["innodb_status"])
	slaveStatusLen := len(m.config.Metrics["slave_status"])

	globalStatus := make(map[string]float64)
	innodbStatus := make(map[string]float64)
	slaveStatus := make(map[string]float64)

	metrics := make([]Metric, 0)

	now, err = FixedTimezone(time.Now(), m.config.Timezone)
	if err != nil {
		m.log.Debug(err)
	}

	if globalStatusLen > 0 {
		globalStatus, err = m.showGlobalStatus()
		if err != nil {
			return metrics, err
		}

		if _, ok := globalStatus["Com_select"]; ok && globalStatus["Com_select"] > 0 {
			globalStatus["Com_select"]--
		}
		setMetrics(&metrics, m.config.Metrics["global_status"], globalStatus, now)
	}

	if innodbStatusLen > 0 {
		innodbStatus, err = m.showEngineInnodbStatus()

		if err != nil {
			return metrics, err
		}

		if _, ok := globalStatus["Com_select"]; ok && globalStatus["Com_select"] > 0 {
			globalStatus["Com_select"]--
		}

		setMetrics(&metrics, m.config.Metrics["innodb_status"], innodbStatus, now)
	}

	if slaveStatusLen > 0 {
		slaveStatus, err = m.showSlaveStatus()
		if err != nil {
			return metrics, err
		}

		if _, ok := globalStatus["Com_select"]; ok && globalStatus["Com_select"] > 0 {
			globalStatus["Com_select"]--
		}

		setMetrics(&metrics, m.config.Metrics["slave_status"], slaveStatus, now)
	}

	return metrics, err
}

func (m *MySQL) Teardown() {
	_ = m.db.Close()
}
