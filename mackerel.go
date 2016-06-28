package main

import (
	"github.com/BurntSushi/toml"
	mkr "github.com/mackerelio/mackerel-client-go"
)

type Mackerel struct {
	client *mkr.Client
	config MackerelConfig
	log    Logger
}

type MackerelConfig struct {
	Service string `toml:"service"`
	APIKey  string `toml:"api_key"`
}

func NewMackerel(mkrConfig MackerelConfig, filename string, log Logger) (Output, error) {
	var err error
	var mackerel *Mackerel
	var str string
	str, err = LoadFile(filename)

	if err != nil {
		return mackerel, err
	}

	var config MackerelConfig
	_, err = toml.Decode(str, &config)

	if err != nil {
		return mackerel, err
	}

	if mkrConfig.APIKey != "" {
		config.APIKey = mkrConfig.APIKey
	}

	if mkrConfig.Service != "" {
		config.Service = mkrConfig.Service
	}

	mackerel = &Mackerel{
		client: mkr.NewClient(config.APIKey),
		config: config,
		log:    log,
	}

	return mackerel, err
}

func (m *Mackerel) Send(metrics []Metric) error {
	var err error

	mkrMetrics := make([]*mkr.MetricValue, 0, len(metrics))
	for _, m := range metrics {
		mkrMetrics = append(mkrMetrics, &mkr.MetricValue{
			Name:  m.Name,
			Time:  m.Time.Unix(),
			Value: m.Value.(float64),
		})
	}
	err = m.client.PostServiceMetricValues(m.config.Service, mkrMetrics)

	return err
}
