package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
	"gitlab.enkod.tech/pkg/kafka/internal/entity"
	"gitlab.enkod.tech/pkg/kafka/internal/logic"
	"gitlab.enkod.tech/pkg/kafka/pkg/logger"
)

func NewClient(
	producerConfig kafka.ConfigMap,
	consumerConfig kafka.ConfigMap,
	serviceName string,
	log *logrus.Logger,
	prefix string,
) entity.Client {
	return logic.NewClient(producerConfig, consumerConfig, serviceName, log, prefix)
}

func Start(client entity.Client) {
	log := logger.GetLogger()
	log.Info("START CONNECTING TO KAFKA")
	err := client.Start()
	if err != nil {
		log.Fatal(err, "can't start kafka client")
	}
}
