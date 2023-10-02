package main

import (
	log "github.com/sirupsen/logrus"
	"kafka_client/internal/app"
	"kafka_client/pkg/config"
	"os"
)

const (
	serviceName = "test_kafka_client"
)

func main() {
	configSettings, err := config.LoadConfigSettingsByPath("config")
	if err != nil {
		log.Error(err)
		os.Exit(2)
	}
	app.Run(configSettings, serviceName)
}
