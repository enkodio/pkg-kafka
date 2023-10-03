package app

import (
	"context"
	kafka "gitlab.enkod.tech/pkg/kafka"
	configEntity "gitlab.enkod.tech/pkg/kafka/pkg/config/entity"
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

func getTestMiddleware() kafka.MiddlewareFunc {
	return func(next kafka.MessageHandler) kafka.MessageHandler {
		return func(ctx context.Context, message kafka.CustomMessage) error {
			kafka.GetLogger().Info("got middleware")
			return next(ctx, message)
		}
	}
}

func testConsumer(topic string, k kafka.Client) {
	k.Subscribe(testHandler, 1, &kafka.TopicSpecifications{
		NumPartitions:     1,
		ReplicationFactor: 1,
		Topic:             topic,
	})
}

func testProducer(topic string, k kafka.Client) {
	err := k.Publish(context.Background(), topic, nil,
		nil,
	)
	if err != nil {
		kafka.GetLogger().WithError(err).Error("produce err")
	}
}

func testHandler(ctx context.Context, msg []byte) error {
	kafka.GetLogger().Info(string(msg))
	return nil
}
