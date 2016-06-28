package main

import (
	"github.com/Sirupsen/logrus"
	"os"
)

type Logger struct {
	log *logrus.Logger
}

func openFile(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
}

func NewLogger() Logger {
	return Logger{
		log: logrus.New(),
	}
}

func (l *Logger) Setup(loglevel, logfile string) {
	defaultLoglevel := "warn"
	if loglevel == "" {
		loglevel = defaultLoglevel
	}

	var err error
	var level logrus.Level
	level, err = logrus.ParseLevel(loglevel)
	if err != nil {
		l.Warn(err)
		level, _ = logrus.ParseLevel(defaultLoglevel)
	}

	l.log.Level = level

	if logfile != "" {
		l.log.Out, err = openFile(logfile)
		l.log.Formatter = &logrus.TextFormatter{DisableColors: true}
	}
}

func (l *Logger) Debug(args ...interface{}) {
	l.log.Debug(args...)
}

func (l *Logger) Info(args ...interface{}) {
	l.log.Info(args...)
}

func (l *Logger) Warn(args ...interface{}) {
	l.log.Warn(args...)
}

func (l *Logger) Error(args ...interface{}) {
	l.log.Error(args...)
}

func (l *Logger) Fatal(args ...interface{}) {
	l.log.Fatal(args...)
}

func (l *Logger) Panic(args ...interface{}) {
	l.log.Panic(args...)
}
