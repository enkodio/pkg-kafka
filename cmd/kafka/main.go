package main

import (
	"github.com/enkodio/pkg-kafka/internal/app"
	"github.com/enkodio/pkg-kafka/pkg/config"
	log "github.com/sirupsen/logrus"
	"os"
)

const (
	serviceName = "test_kafka_client"
)

func main() {
	configSettings, err := config.LoadConfigSettingsByPath("configs")
	if err != nil {
		log.Error(err)
		os.Exit(2)
	}
	app.Run(configSettings, serviceName)
}
