package examples

import (
	kafkaClient "github.com/enkodio/pkg-kafka/client"
	configEntity "github.com/enkodio/pkg-kafka/internal/pkg/config/entity"
	"github.com/enkodio/pkg-kafka/kafka"
)

func RunBase(configSettings configEntity.Settings, serviceName string) {
	const (
		testTopic = "test_topic"
	)

	//broker clients
	var (
		k = kafkaClient.NewClient(configSettings.KafkaProducer, configSettings.KafkaConsumer, serviceName, nil, "")
	)

	testConsumer(k, testHandler("base"), kafka.TopicSpecifications{
		Topic:             testTopic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	k.Pre(
		getTestMiddleware(),
	)
	kafkaClient.Start(k)
	testProducer(k, testTopic, "test")
	testProducer(k, testTopic, "test_2")
	k.StopProduce()
	testProducer(k, testTopic, "after_stop")
}

func RunWithKey(configSettings configEntity.Settings, serviceName string) {
	const (
		testTopic = "test_topic_with_key"
	)

	//broker clients
	var (
		k = kafkaClient.NewClient(configSettings.KafkaProducer, configSettings.KafkaConsumer, serviceName, nil, "")
	)

	testConsumer(k, testHandler("first consumer"), kafka.TopicSpecifications{
		Topic:             testTopic,
		NumPartitions:     2,
		ReplicationFactor: 1,
	})
	testConsumer(k, testHandler("second consumer"), kafka.TopicSpecifications{
		Topic:             testTopic,
		NumPartitions:     2,
		ReplicationFactor: 1,
	})

	k.Pre(
		getTestMiddleware(),
	)
	kafkaClient.Start(k)
	testProducer(k, testTopic, publishingData{
		Value: "1",
		Key:   "1",
	})
	testProducer(k, testTopic, publishingData{
		Value: "2",
		Key:   "asdasd",
	})
	testProducer(k, testTopic, publishingData{
		Value: "2",
		Key:   "asdasd",
	})
	k.StopProduce()
}

func RunWithPublicPublishingData(configSettings configEntity.Settings, serviceName string) {
	const (
		testTopic = "test_topic_public_publishing_data"
	)

	//broker clients
	var (
		k = kafkaClient.NewClient(configSettings.KafkaProducer, configSettings.KafkaConsumer, serviceName, nil, "")
	)

	testConsumer(k, testHandler("first consumer"), kafka.TopicSpecifications{
		Topic:             testTopic,
		NumPartitions:     2,
		ReplicationFactor: 1,
	})
	testConsumer(k, testHandler("second consumer"), kafka.TopicSpecifications{
		Topic:             testTopic,
		NumPartitions:     2,
		ReplicationFactor: 1,
	})

	k.Pre(
		getTestMiddleware(),
	)
	kafkaClient.Start(k)
	testProducer(k, testTopic, kafka.PublicationData{
		Value: []byte("1"),
		Key:   "1",
	})
	testProducer(k, testTopic, kafka.PublicationData{
		Value: []byte("2"),
		Key:   "asdasd",
	})
	testProducer(k, testTopic, kafka.PublicationData{
		Value: []byte("2"),
		Key:   "asdasd",
	})
	k.StopProduce()
}
