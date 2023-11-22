package client

import (
	cKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/enkodio/pkg-kafka/internal/pkg/logger"
	"github.com/enkodio/pkg-kafka/kafka"
	"github.com/sirupsen/logrus"
)

func NewClient(
	producerConfig cKafka.ConfigMap,
	consumerConfig cKafka.ConfigMap,
	serviceName string,
	log *logrus.Logger,
	prefix string,
) kafka.Client {
	if log != nil {
		logger.SetLogger(log)
	} else {
		logger.SetDefaultLogger("debug")
	}

	return newClient(producerConfig, consumerConfig, serviceName, prefix)
}

func Start(client kafka.Client) {
	log := logger.GetLogger()
	log.Info("START CONNECTING TO KAFKA")
	err := client.Start()
	if err != nil {
		log.Fatal(err, "can't start kafka client")
	}
}
