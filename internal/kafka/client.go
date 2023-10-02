package kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"kafka_client/internal/entity"
	"kafka_client/pkg/logger"
	"time"
)

const (
	// Максимальное количество реплик каждой партиции (равно количеству брокеров в кластере)
	maxReplicationFactor = 3

	// Значение реплик каждой партиции по умолчанию
	defaultReplicationFactor = 1
	// Значение партиций для топика по умолчанию
	defaultNumPartitions = 3

	// Время ожидания, пока очередь в буфере продусера переполнена
	queueFullWaitTime = time.Second * 5

	reconnectTime = time.Second * 10

	flushTimeout = 5000

	readTimeout = time.Second
)

type client struct {
	serviceName string
	topicPrefix string
	consumers   consumers
	producer    *producer
}

func NewBrokerClient(
	producerConfig kafka.ConfigMap,
	consumerConfig kafka.ConfigMap,
	serviceName string,
) entity.BrokerClient {
	consumerConfig["group.id"] = serviceName
	return &client{
		serviceName: serviceName,
		producer:    newProducer(producerConfig),
		consumers:   newConsumers(consumerConfig),
	}
}

func Start(client entity.BrokerClient) {
	log := logger.GetLogger()
	log.Info("START CONNECTING TO KAFKA")
	err := client.Start()
	if err != nil {
		log.Fatal(err, "can't start kafka client")
	}
}

func (c *client) Start() (err error) {
	err = c.producer.initProducer()
	if err != nil {
		return
	}
	if len(c.consumers.consumers) != 0 {
		err = c.producer.createTopics(c.consumers.getUniqByNameTopicSpecifications())
		if err != nil {
			return
		}
	}
	c.consumers.initConsumers()
	return
}

func (c *client) Pre(mw ...entity.MiddlewareFunc) {
	for _, v := range mw {
		c.consumers.mwFuncs = append(c.consumers.mwFuncs, v)
	}
}

func (c *client) StopSubscribe() {
	c.consumers.stopConsumers()
}

func (c *client) StopProduce() {
	c.producer.stop()
}

func (c *client) Publish(ctx context.Context, message entity.Message) (err error) {
	return c.publish(ctx, message)
}

func (c *client) Subscribe(h entity.Handler, countConsumers int, spec entity.TopicSpecifications) {
	log := logger.GetLogger()
	spec.Topic = c.topicPrefix + spec.Topic
	for j := 0; j < countConsumers; j++ {
		err := c.consumers.addNewConsumer(h, spec)
		if err != nil {
			log.Fatal(err, "can't create new consumer")
		}
	}
}

func (c *client) publish(ctx context.Context, message entity.Message) (err error) {
	message.Topic = c.topicPrefix + message.Topic
	deliveryChannel := make(chan kafka.Event)

	go c.handleDelivery(ctx, message, deliveryChannel)

	err = c.producer.produce(
		ctx,
		message.ToKafkaMessage(),
		deliveryChannel,
	)
	if err != nil {
		return err
	}
	return
}

func (c *client) handleDelivery(ctx context.Context, message entity.Message, deliveryChannel chan kafka.Event) {
	log := logger.FromContext(ctx)
	e := <-deliveryChannel
	close(deliveryChannel)
	switch event := e.(type) {
	case *kafka.Message:
		if event.TopicPartition.Error != nil {
			kafkaErr := event.TopicPartition.Error.(kafka.Error)
			// Если retriable, то ошибка временная, нужно пытаться переотправить снова, если нет, то ошибка nonretriable, просто логируем
			if kafkaErr.IsRetriable() {
				log.WithError(kafkaErr).
					Errorf("kafka produce retriable error, try again send topic: %v, message: %v",
						message.Topic, string(message.Body))
				err := c.publish(ctx, message)
				if err != nil {
					log.WithError(err).
						Errorf("Cant publish by kafka, topic: %v, message: %v",
							message.Topic, string(message.Body))
				}
			} else {
				log.WithError(kafkaErr).
					Errorf("kafka produce nonretriable error, can't send topic: %v, message: %v. Is fatal: %v",
						message.Topic, string(message.Body), kafkaErr.IsFatal())
			}
		}
	case kafka.Error:
		// Общие пользовательские ошибки, клиент сам пытается переотправить, просто логируем
		log.WithError(event).
			Errorf("publish error, topic: %v, message: %v. client tries to send again", message.Topic, string(message.Body))
	}
}
