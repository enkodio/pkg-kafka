package client

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gitlab.enkod.tech/pkg/kafka/entity"
	"gitlab.enkod.tech/pkg/kafka/logger"
	"time"
)

const (
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

func (c *client) Publish(ctx context.Context, customMessage entity.CustomMessage) (err error) {
	message := entity.NewMessage(customMessage)
	message.Topic = c.topicPrefix + message.Topic
	message.Headers.SetHeader(entity.ServiceNameHeaderKey, []byte(c.serviceName))
	return c.producer.publish(ctx, message)
}

func (c *client) Subscribe(h entity.Handler, countConsumers int, specification entity.Specifications) {
	log := logger.GetLogger()
	topicSpecification := entity.NewTopicSpecifications(specification)
	topicSpecification.Topic = c.topicPrefix + topicSpecification.Topic
	for j := 0; j < countConsumers; j++ {
		err := c.consumers.addNewConsumer(h, topicSpecification)
		if err != nil {
			log.Fatal(err, "can't create new consumer")
		}
	}
}
