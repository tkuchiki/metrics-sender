package main

import (
	"bufio"
	"github.com/BurntSushi/toml"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type Command struct {
	config CommandConfig
	log    Logger
}

type CommandConfig struct {
	Command  string `toml:"command"`
	Timezone string `toml:"timezone"`
}

func NewCommand(cmdConfig CommandConfig, filename string, log Logger) (Input, error) {
	var err error
	var c *Command
	var str string
	str, err = LoadFile(filename)

	if err != nil {
		return c, err
	}

	var config CommandConfig
	_, err = toml.Decode(str, &config)

	if err != nil {
		return c, err
	}

	if cmdConfig.Command != "" {
		config.Command = cmdConfig.Command
	}

	if cmdConfig.Timezone != "" {
		config.Timezone = cmdConfig.Timezone
	}

	c = &Command{
		config: config,
		log:    log,
	}

	return c, err
}

func (cmd *Command) runCommand() (map[string]Stat, error) {
	var err error
	var now time.Time
	var data map[string]Stat
	commands := strings.Fields(cmd.config.Command)

	var c *exec.Cmd
	if len(commands) == 1 {
		c = exec.Command(commands[0])
	} else {
		c = exec.Command(commands[0], commands[1:]...)
	}

	stdout, err := c.StdoutPipe()

	if err != nil {
		return data, err
	}

	c.Start()

	data = make(map[string]Stat)
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		lines := strings.Fields(scanner.Text())
		var fval float64
		fval, err = strconv.ParseFloat(lines[1], 64)
		if err != nil {
			cmd.log.Debug(err)
			continue
		}

		if len(lines) > 2 {
			var timestamp int64
			timestamp, err = strconv.ParseInt(lines[2], 10, 64)
			if err != nil {
				cmd.log.Debug(err)
				continue
			}
			t := time.Unix(timestamp, 0)
			now, err = FixedTimezone(t, cmd.config.Timezone)
		}
		if len(lines) == 2 || err != nil {
			now, err = FixedTimezone(time.Now(), cmd.config.Timezone)
			if err != nil {
				cmd.log.Debug(err)
				continue
			}
		}

		data[lines[0]] = Stat{
			Value: fval,
			Time:  now,
		}
	}

	c.Wait()

	return data, err
}

func (cmd *Command) FetchMetrics() ([]Metric, error) {
	var err error

	stats, err := cmd.runCommand()
	metrics := make([]Metric, 0, len(stats))

	for name, s := range stats {
		metrics = append(metrics, Metric{
			Name:  name,
			Value: s.Value,
			Time:  s.Time,
		})
	}

	return metrics, err
}

func (cmd *Command) Teardown() {

}
