package main

import (
	"github.com/BurntSushi/toml"
	"io/ioutil"
)

type Config struct {
	InputType  string `toml:"input_type"`
	OutputType string `toml:"output_type"`
	LogFile    string `toml:"log_file"`
	LogLevel   string `toml:"log_level"`
	Target     string `toml:"target"`
}

func LoadFile(filename string) (string, error) {
	var err error
	buf, err := ioutil.ReadFile(filename)

	return string(buf), err
}

func LoadConfig(c Config, filename string) (Config, error) {
	var err error
	var config Config
	str, err := LoadFile(filename)
	if err != nil {
		return config, err
	}

	_, err = toml.Decode(str, &config)

	if c.InputType != "" {
		config.InputType = c.InputType
	}

	if c.OutputType != "" {
		config.OutputType = c.OutputType
	}

	if c.LogFile != "" {
		config.LogFile = c.LogFile
	}

	if c.LogLevel != "" {
		config.LogLevel = c.LogLevel
	}

	if c.Target != "" {
		config.Target = c.Target
	}

	return config, err
}
