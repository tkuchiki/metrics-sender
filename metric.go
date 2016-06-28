package main

import (
	"fmt"
	"strings"
	"time"
)

type MetricsConfig struct {
	Name   string  `toml:"name"`
	Alias  string  `toml:"alias"`
	Prefix string  `toml:"prefix"`
	Unit   float64 `toml:"unit"`
	names  []string
}

type Metric struct {
	Name  string      `json:"name"`
	Time  time.Time   `json:"time"`
	Value interface{} `json:"value"`
}

type Stat struct {
	Value float64
	Time  time.Time
}

func (mc *MetricsConfig) SplitName() []string {
	names := strings.Split(mc.Name, ",")
	mc.names = make([]string, 0, len(names))

	for _, n := range names {
		mc.names = append(mc.names, strings.Trim(n, " "))
	}

	return mc.names
}

func (mc *MetricsConfig) namesLen() int {
	return len(mc.names)
}

func (mc *MetricsConfig) CreateName(name string) string {
	if name == "" {
		name = mc.Name
	}

	if mc.namesLen() == 1 && mc.Alias != "" {
		return mc.Alias
	}

	if mc.Prefix != "" {
		return fmt.Sprintf("%s.%s", mc.Prefix, name)
	}

	return name
}

func (mc *MetricsConfig) CalcValue(value float64) float64 {
	if mc.Unit != 0 {
		return value / mc.Unit
	}

	return value
}
