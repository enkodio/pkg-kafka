package main

import (
	"github.com/enkodio/pkg-kafka/internal/kafka/app"
	"github.com/enkodio/pkg-kafka/internal/pkg/config"
	log "github.com/sirupsen/logrus"
	"os"
)

const (
	serviceName = "test_kafka_client"
)

func main() {
	configSettings, err := config.LoadConfigSettingsByPath("internal/cmd/configs")
	if err != nil {
		log.Error(err)
		os.Exit(2)
	}
	app.Run(configSettings, serviceName)
}
