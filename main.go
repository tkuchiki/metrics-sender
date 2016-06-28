package main

import (
	"fmt"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"path/filepath"
)

var (
	configFile = kingpin.Flag("config", "Config file").Short('c').Required().String()
	inputConf  = kingpin.Flag("input-config", "Input config file").Required().String()
	outputConf = kingpin.Flag("output-config", "Output config file").Required().String()
	target     = kingpin.Flag("target", "Send target").String()
	inType     = kingpin.Flag("input-type", "Input type").String()
	outType    = kingpin.Flag("output-type", "Output type").String()
	logFile    = kingpin.Flag("logfile", "Logfile").String()
	logLevel   = kingpin.Flag("loglevel", "Loglevel").String()
	timezone   = kingpin.Flag("timezone", "Timezone").String()

	inPort   = kingpin.Flag("input-port", "Input port").Int()
	outPort  = kingpin.Flag("output-port", "Output port").Int()
	inHost   = kingpin.Flag("input-host", "Input host").String()
	outHost  = kingpin.Flag("output-host", "Output host").String()
	password = kingpin.Flag("password", "Password").String()
	timeout  = kingpin.Flag("timeout", "Timeout").Default("5").Int()

	// cloudwatch
	awsAccessKeyID     = kingpin.Flag("access-key", "AWS access key ID").String()
	awsSecretAccessKey = kingpin.Flag("secret-key", "AWS secret access key").String()
	token              = kingpin.Flag("token", "AWS access token").String()
	profile            = kingpin.Flag("profile", "AWS CLI profile").String()
	creds              = kingpin.Flag("credentials", "AWS CLI Credentials").String()

	// command
	cmd = kingpin.Flag("command", "Command").String()

	// mackerel
	mkrAPIKey = kingpin.Flag("mackerel-api-key", "Mackerel API Key").String()

	// buffer
	bufferPath = kingpin.Flag("buffer-path", "Buffer path").String()
	bufferMode = kingpin.Flag("buffer-mode", "Buffer file mode").Default("0600").String()
)

func main() {
	kingpin.Version("0.1.0")
	kingpin.Parse()

	os.Setenv("NSS_SDB_USE_CACHE", "yes")

	log := NewLogger()

	argConfig := Config{
		InputType:  *inType,
		OutputType: *outType,
		LogFile:    *logFile,
		LogLevel:   *logLevel,
		Target:     *target,
	}

	config, err := LoadConfig(argConfig, *configFile)

	if err != nil {
		log.Fatal(err)
	}

	log.Setup(config.LogLevel, config.LogFile)

	var input Input
	switch config.InputType {
	case "cloudwatch":
		cwConfig := CloudWatchConfig{
			AWSAccessKeyId:     *awsAccessKeyID,
			AWSSecretAccessKey: *awsSecretAccessKey,
			Token:              *token,
			Profile:            *profile,
			Credentials:        *creds,
			Timezone:           *timezone,
		}
		input, err = NewCloudWatch(cwConfig, *inputConf, log)
	case "mysql":
		mysqlConfig := MySQLConfig{
			Host:     *inHost,
			Port:     *inPort,
			Password: *password,
			Timeout:  *timeout,
			Timezone: *timezone,
		}
		input, err = NewMySQL(mysqlConfig, *inputConf, log)
	case "redis":
		redisConfig := RedisConfig{
			Port:     *inPort,
			Password: *password,
			Timeout:  *timeout,
			Timezone: *timezone,
		}
		input, err = NewRedis(redisConfig, *inputConf, log)
	case "command":
		cmdConfig := CommandConfig{
			Command:  *cmd,
			Timezone: *timezone,
		}
		input, err = NewCommand(cmdConfig, *inputConf, log)
	default:
		log.Fatal(fmt.Sprintf("Invalid input type: %s", config.InputType))
	}

	defer input.Teardown()

	if err != nil {
		log.Fatal(err)
	}

	var output Output
	switch config.OutputType {
	case "zabbix":
		zbxConfig := ZabbixConfig{
			Server: *outHost,
			Port:   *outPort,
			Host:   *target,
		}
		output, err = NewZabbix(zbxConfig, *outputConf, log)
	case "mackerel":
		mkrConfig := MackerelConfig{
			APIKey:  *mkrAPIKey,
			Service: *target,
		}
		output, err = NewMackerel(mkrConfig, *outputConf, log)
	default:
		log.Fatal(fmt.Sprintf("Invalid output type: %s", config.OutputType))
	}

	bPath := filepath.Join(os.TempDir(), fmt.Sprintf("%s_metrics.db", config.Target))

	if err != nil {
		log.Fatal(err)
	}

	metrics, err := input.FetchMetrics()
	if err != nil {
		log.Fatal(err)
	}
	log.Debug("metrics: ", metrics)

	buffer, bufErr := NewBuffer(bPath, *bufferMode)
	defer buffer.Close()
	bucket := config.InputType

	var bufferedMetrics map[string][]Metric
	bufferedMetrics, err = buffer.Read(bucket, 10)

	if err != nil {
		if err.Error() != "Bucket not found" {
			log.Warn(err)
		}
	} else {
		if len(bufferedMetrics) > 0 {
			for key, ms := range bufferedMetrics {
				err = output.Send(ms)
				if err != nil {
					log.Warn(err)
				} else {
					err = buffer.Delete(bucket, key)
					log.Debug(err)
				}
			}

		}
	}
	log.Debug("bufferd metrics: ", bufferedMetrics)

	err = output.Send(metrics)
	if err != nil {
		if bufErr != nil {
			log.Warn(err)
		} else {
			err = buffer.Write(bucket, metrics)
			if err != nil {
				log.Warn(err)
			}
		}

		log.Fatal(err)
	}
}
