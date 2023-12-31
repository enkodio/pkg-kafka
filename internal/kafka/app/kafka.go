package app

import (
	"context"
	kafkaClient "github.com/enkodio/pkg-kafka/client"
	"github.com/enkodio/pkg-kafka/internal/kafka/entity"
	configEntity "github.com/enkodio/pkg-kafka/internal/pkg/config/entity"
	"github.com/enkodio/pkg-kafka/internal/pkg/logger"
	"github.com/enkodio/pkg-kafka/kafka"
)

func Run(configSettings configEntity.Settings, serviceName string) {
	const (
		testTopic = "test_topic"
	)

	//broker clients
	var (
		k = kafkaClient.NewClient(configSettings.KafkaProducer, configSettings.KafkaConsumer, serviceName, nil, "")
	)

	testConsumer(testTopic, k)
	k.Pre(
		getTestMiddleware(),
	)
	kafkaClient.Start(k)
	testProducer(testTopic, k)
	testProducer(testTopic, k)
	k.StopProduce()
	testProducer(testTopic, k)
	select {}
}

func getTestMiddleware() kafka.MiddlewareFunc {
	return func(next kafka.MessageHandler) kafka.MessageHandler {
		return func(ctx context.Context, message kafka.Message) error {
			logger.GetLogger().Info("got middleware")
			return next(ctx, message)
		}
	}
}

func testConsumer(topic string, k kafka.Client) {
	k.Subscribe(testHandler, 1, &entity.TopicSpecifications{
		NumPartitions:     1,
		ReplicationFactor: 1,
		Topic:             topic,
	})
}

func testProducer(topic string, k kafka.Client) {
	err := k.Publish(context.Background(), topic, "test", map[string][]byte{
		"test": []byte("test"),
	})
	if err != nil {
		logger.GetLogger().WithError(err).Error("produce err")
	}
}

func testHandler(ctx context.Context, msg []byte) error {
	logger.GetLogger().Info(string(msg))
	return nil
}
