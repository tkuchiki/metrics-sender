package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/BurntSushi/toml"
	zabbix "github.com/blacked/go-zabbix"
	"regexp"
	"strconv"
)

type Zabbix struct {
	sender    *zabbix.Sender
	config    ZabbixConfig
	log       Logger
	Processed int
	Failed    int
}

type ZabbixConfig struct {
	Server string `toml:"server"`
	Port   int    `toml:"port"`
	Host   string `toml:"host"`
}

type ZabbixResponse struct {
	Response string `json:"response"`
	Info     string `json:"info"`
}

func NewZabbix(zbxConfig ZabbixConfig, filename string, log Logger) (Output, error) {
	var err error
	var zbx *Zabbix
	var str string
	str, err = LoadFile(filename)

	if err != nil {
		return zbx, err
	}

	var config ZabbixConfig
	config.Port = 10051
	_, err = toml.Decode(str, &config)

	if err != nil {
		return zbx, err
	}

	if zbxConfig.Server != "" {
		config.Server = zbxConfig.Server
	} else {
		config.Server = "localhost"
	}

	if zbxConfig.Port > 0 {
		config.Port = zbxConfig.Port
	}

	if zbxConfig.Host != "" {
		config.Host = zbxConfig.Host
	}

	zbx = &Zabbix{
		sender: zabbix.NewSender(config.Server, config.Port),
		config: config,
		log:    log,
	}

	return zbx, err
}

func round(f float64) string {
	return fmt.Sprintf("%.4f", f)
}

func (z *Zabbix) convertMetrics(metrics []Metric) []*zabbix.Metric {
	zbxMetrics := make([]*zabbix.Metric, 0, len(metrics))

	var value string

	for _, m := range metrics {
		switch m.Value.(type) {
		case string:
			value = m.Value.(string)
		case float64:
			value = round(m.Value.(float64))
		}

		zbxMetrics = append(zbxMetrics, zabbix.NewMetric(z.config.Host, m.Name, value, m.Time.Unix()))
	}

	return zbxMetrics
}

func (z *Zabbix) Send(metrics []Metric) error {
	var err error

	zbxMetrics := z.convertMetrics(metrics)

	packet := zabbix.NewPacket(zbxMetrics)
	buf := z.sender.Send(packet)

	header := "ZBXD\x01"

	if header != string(buf[:5]) {
		return errors.New("Invalid header")
	}

	resp := ZabbixResponse{}
	err = json.Unmarshal(buf[13:], &resp)

	if err == nil {
		re := regexp.MustCompile(`Processed (\d+) Failed (\d+)`)
		m := re.FindStringSubmatch(resp.Info)
		processed, _ := strconv.Atoi(m[1])
		z.Processed = processed
		failed, _ := strconv.Atoi(m[2])
		z.Failed = failed

		if processed == 0 {
			return errors.New("Failed to send to Zabbix Server")
		}
	}

	return err
}
