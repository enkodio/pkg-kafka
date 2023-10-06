package client

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
	"gitlab.enkod.tech/pkg/kafka"
	"gitlab.enkod.tech/pkg/kafka/internal/logic"
	"gitlab.enkod.tech/pkg/kafka/pkg/logger"
)

func NewClient(
	producerConfig kafka.ConfigMap,
	consumerConfig kafka.ConfigMap,
	serviceName string,
	log *logrus.Logger,
	prefix string,
) kafka_client.Client {
	if log != nil {
		logger.SetLogger(log)
	} else {
		logger.SetDefaultLogger("debug")
	}

	return logic.NewClient(producerConfig, consumerConfig, serviceName, prefix)
}

func Start(client kafka_client.Client) {
	log := logger.GetLogger()
	log.Info("START CONNECTING TO KAFKA")
	err := client.Start()
	if err != nil {
		log.Fatal(err, "can't start kafka client")
	}
}
