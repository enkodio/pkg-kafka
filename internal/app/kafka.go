package app

import (
	"context"
	kafkaClient "gitlab.enkod.tech/pkg/kafka/client"
	configEntity "gitlab.enkod.tech/pkg/kafka/pkg/config/entity"
	"gitlab.enkod.tech/pkg/kafka/pkg/logger"
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

func getTestMiddleware() kafkaClient.MiddlewareFunc {
	return func(next kafkaClient.MessageHandler) kafkaClient.MessageHandler {
		return func(ctx context.Context, message kafkaClient.Message) error {
			logger.GetLogger().Info("got middleware")
			return next(ctx, message)
		}
	}
}

func testConsumer(topic string, k kafkaClient.Client) {
	k.Subscribe(testHandler, 1, &kafkaClient.TopicSpecifications{
		NumPartitions:     1,
		ReplicationFactor: 1,
		Topic:             topic,
	})
}

func testProducer(topic string, k kafkaClient.Client) {
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
