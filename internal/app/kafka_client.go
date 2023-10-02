package app

import (
	"context"
	"kafka_client/internal/entity"
	kafkaClient "kafka_client/internal/kafka"
	configEntity "kafka_client/pkg/config/entity"
	"kafka_client/pkg/logger"
)

func Run(configSettings configEntity.Settings, serviceName string) {
	logger.SetDefaultLogger("debug")

	const (
		testTopic = "test_topic"
	)

	//broker clients
	var (
		k = kafkaClient.NewBrokerClient(configSettings.KafkaProducer, configSettings.KafkaConsumer, serviceName)
	)

	testConsumer(testTopic, k)
	k.Pre(
		getTestMiddleware(),
	)
	kafkaClient.Start(k)
	testProducer(testTopic, k)
	k.StopSubscribe()
	k.StopProduce()
	testProducer(testTopic, k)
	select {}
}

func getTestMiddleware() entity.MiddlewareFunc {
	return func(next entity.MessageHandler) entity.MessageHandler {
		return func(ctx context.Context, message entity.Message) error {
			logger.GetLogger().Info("got middleware")
			return next(ctx, message)
		}
	}
}

func testConsumer(topic string, k entity.BrokerClient) {
	k.Subscribe(testHandler, 1, entity.TopicSpecifications{
		NumPartitions:     1,
		ReplicationFactor: 1,
		Topic:             topic,
	})
}

func testProducer(topic string, k entity.BrokerClient) {
	err := k.Publish(context.Background(), entity.Message{
		Topic: topic,
		Body:  []byte("test"),
	})
	if err != nil {
		logger.GetLogger().WithError(err).Error("produce err")
	}
}

func testHandler(ctx context.Context, msg []byte) error {
	logger.GetLogger().Info(string(msg))
	return nil
}