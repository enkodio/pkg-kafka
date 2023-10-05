package app

import (
	"context"
	"gitlab.enkod.tech/pkg/kafka"
	"gitlab.enkod.tech/pkg/kafka/internal/entity"
	configEntity "gitlab.enkod.tech/pkg/kafka/pkg/config/entity"
	"gitlab.enkod.tech/pkg/kafka/pkg/logger"
)

func Run(configSettings configEntity.Settings, serviceName string) {
	const (
		testTopic = "test_topic"
	)

	//broker clients
	var (
		k = kafka.NewClient(configSettings.KafkaProducer, configSettings.KafkaConsumer, serviceName, nil, "")
	)

	testConsumer(testTopic, k)
	k.Pre(
		getTestMiddleware(),
	)
	kafka.Start(k)
	testProducer(testTopic, k)
	testProducer(testTopic, k)
	k.StopProduce()
	testProducer(testTopic, k)
	select {}
}

func getTestMiddleware() entity.MiddlewareFunc {
	return func(next entity.MessageHandler) entity.MessageHandler {
		return func(ctx context.Context, message entity.CustomMessage) error {
			logger.GetLogger().Info("got middleware")
			return next(ctx, message)
		}
	}
}

func testConsumer(topic string, k entity.Client) {
	k.Subscribe(testHandler, 1, &entity.TopicSpecifications{
		NumPartitions:     1,
		ReplicationFactor: 1,
		Topic:             topic,
	})
}

func testProducer(topic string, k entity.Client) {
	err := k.Publish(context.Background(), topic, nil,
		nil,
	)
	if err != nil {
		logger.GetLogger().WithError(err).Error("produce err")
	}
}

func testHandler(ctx context.Context, msg []byte) error {
	logger.GetLogger().Info(string(msg))
	return nil
}
