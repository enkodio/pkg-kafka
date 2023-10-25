package client

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
	"gitlab.enkod.tech/pkg/kafka/pkg/logger"
)

func NewClient(
	producerConfig kafka.ConfigMap,
	consumerConfig kafka.ConfigMap,
	serviceName string,
	log *logrus.Logger,
	prefix string,
) Client {
	if log != nil {
		logger.SetLogger(log)
	} else {
		logger.SetDefaultLogger("debug")
	}

	return newClient(producerConfig, consumerConfig, serviceName, prefix)
}

func Start(client Client) {
	log := logger.GetLogger()
	log.Info("START CONNECTING TO KAFKA")
	err := client.Start()
	if err != nil {
		log.Fatal(err, "can't start kafka client")
	}
}
