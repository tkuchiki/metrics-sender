package main

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"gopkg.in/redis.v3"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Redis struct {
	client *redis.Client
	config RedisConfig
	log    Logger
}

type RedisConfig struct {
	Host     string               `toml:"host"`
	Port     int                  `toml:"port"`
	Password string               `toml:"password"`
	Timeout  int                  `toml:"timeout"`
	Timezone string               `toml:"timezone"`
	Metrics  []RedisMetricsConfig `toml:"metrics"`
}

type RedisMetricsConfig struct {
	MetricsConfig
}

func NewRedis(redisConfig RedisConfig, filename string, log Logger) (Input, error) {
	var err error
	var r *Redis
	var str string
	str, err = LoadFile(filename)

	if err != nil {
		return r, err
	}

	var config RedisConfig
	_, err = toml.Decode(str, &config)

	if err != nil {
		return r, err
	}

	if redisConfig.Host != "" {
		config.Host = redisConfig.Host
	}

	if redisConfig.Port != 0 {
		config.Port = redisConfig.Port
	} else if config.Port == 0 {
		config.Port = 6379
	}

	if redisConfig.Password != "" {
		config.Password = redisConfig.Password
	}

	if redisConfig.Timeout > 0 {
		config.Timeout = redisConfig.Timeout
	}

	if redisConfig.Timezone != "" {
		config.Timezone = redisConfig.Timezone
	}

	client := redis.NewClient(&redis.Options{
		Addr:        fmt.Sprintf("%s:%d", config.Host, config.Port),
		Password:    config.Password,
		DB:          0,
		DialTimeout: time.Duration(config.Timeout) * time.Second,
		ReadTimeout: time.Duration(config.Timeout) * time.Second,
	})

	r = &Redis{
		client: client,
		config: config,
		log:    log,
	}

	return r, err
}

func (r *Redis) info() (map[string]float64, error) {
	var err error
	var stats map[string]float64
	info := r.client.Info()
	if info.Err() != nil {
		return stats, info.Err()
	}
	stats = make(map[string]float64)
	for _, l := range strings.Split(info.Val(), "\r\n") {
		if strings.Index(l, "#") == 0 || l == "" {
			continue
		}
		kv := strings.SplitN(l, ":", 2)
		key, value := kv[0], kv[1]

		var fval float64
		if strings.Index(value, ",") != -1 { // Keyspace
			re := regexp.MustCompile(`(.+)=(.+),(.+)=(.+),(.+)=(.+)`)
			group := re.FindStringSubmatch(value)
			if len(group) == 0 {
				continue
			}
			keyspaces := map[string]string{
				group[1]: group[2],
				group[3]: group[4],
				group[5]: group[6],
			}

			for k, v := range keyspaces {
				dbKey := fmt.Sprintf("%s_%s", key, k)
				fval, err = strconv.ParseFloat(v, 64)
				if err != nil {
					continue
				}

				stats[dbKey] = fval
				stats[k] += fval
			}
		} else {
			fval, err = strconv.ParseFloat(value, 64)
			if err != nil {
				continue
			}
			stats[key] = fval
		}

	}

	return stats, nil
}

func (r *Redis) FetchMetrics() ([]Metric, error) {
	var err error
	var now time.Time

	stats, err := r.info()
	metrics := make([]Metric, 0, len(r.config.Metrics))

	now, err = FixedTimezone(time.Now(), r.config.Timezone)
	if err != nil {
		r.log.Debug(err)
	}

	for _, m := range r.config.Metrics {
		names := m.SplitName()
		for _, n := range names {
			name := m.CreateName(n)
			value := m.CalcValue(stats[m.Name])

			metrics = append(metrics, Metric{
				Name:  name,
				Value: value,
				Time:  now,
			})
		}
	}

	return metrics, err
}

func (r *Redis) Teardown() {
	_ = r.client.Close()
}
