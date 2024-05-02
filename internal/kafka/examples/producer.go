package examples

import (
	"context"
	"github.com/enkodio/pkg-kafka/internal/pkg/logger"
	"github.com/enkodio/pkg-kafka/kafka"
)

func testProducer(k kafka.Client, topic string, data interface{}, headers ...map[string][]byte) {
	err := k.Publish(context.Background(), topic, data, headers...)
	if err != nil {
		logger.GetLogger().WithError(err).Error("produce err")
	}
}
