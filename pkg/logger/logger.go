package logger

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/vm-affekt/logrustash"

	"time"
)

var logger *logrus.Logger

func SetLogger(l *logrus.Logger) {
	logger = l
}

func GetLogger() *logrus.Logger {
	return logger
}

func SetDefaultLogger(level string) {
	l := logrus.New()
	parsedLever, err := logrus.ParseLevel(level)
	if err != nil {
		fmt.Println("error when definition logging level:", err)
		parsedLever = logrus.DebugLevel
	}
	l.SetLevel(parsedLever)
	l.SetFormatter(
		&logrustash.LogstashFormatter{
			TimestampFormat: time.RFC3339Nano,
		},
	)
	logger = l
	return
}
