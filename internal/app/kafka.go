package app

import (
	"context"
	"gitlab.enkod.tech/pkg/kafka"
	kafka2 "gitlab.enkod.tech/pkg/kafka/kafka"
	configEntity "gitlab.enkod.tech/pkg/kafka/pkg/config/entity"
	"gitlab.enkod.tech/pkg/kafka/pkg/logger"
)

func Run(configSettings configEntity.Settings, serviceName string) {
	const (
		testTopic = "test_topic"
	)

	//broker clients
	var (
		k = kafka2.NewClient(configSettings.KafkaProducer, configSettings.KafkaConsumer, serviceName, nil, "")
	)

	testConsumer(testTopic, k)
	k.Pre(
		getTestMiddleware(),
	)
	kafka2.Start(k)
	testProducer(testTopic, k)
	testProducer(testTopic, k)
	k.StopProduce()
	testProducer(testTopic, k)
	select {}
}

func getTestMiddleware() kafka_client.MiddlewareFunc {
	return func(next kafka_client.MessageHandler) kafka_client.MessageHandler {
		return func(ctx context.Context, message kafka2.CustomMessage) error {
			logger.GetLogger().Info("got middleware")
			return next(ctx, message)
		}
	}
}

func testConsumer(topic string, k kafka_client.Client) {
	k.Subscribe(testHandler, 1, &kafka2.TopicSpecifications{
		NumPartitions:     1,
		ReplicationFactor: 1,
		Topic:             topic,
	})
}

func testProducer(topic string, k kafka_client.Client) {
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
