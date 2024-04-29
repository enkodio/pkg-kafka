package logic

import (
	"context"
	cKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/enkodio/pkg-kafka/internal/kafka/entity"
	"github.com/enkodio/pkg-kafka/internal/pkg/logger"
	"github.com/enkodio/pkg-kafka/kafka"
	"github.com/pkg/errors"
	"time"
)

const (
	// Время ожидания, пока очередь в буфере продусера переполнена
	queueFullWaitTime = time.Second * 5
	reconnectTime     = time.Second * 10
	flushTimeout      = 5000
	readTimeout       = time.Second
)

type Client struct {
	serviceName string
	topicPrefix string
	consumers   consumers
	producer    *producer
}

func NewClient(
	producerConfig cKafka.ConfigMap,
	consumerConfig cKafka.ConfigMap,
	serviceName string,
	prefix string,
) *Client {
	consumerConfig["group.id"] = serviceName
	return &Client{
		serviceName: serviceName,
		producer:    newProducer(producerConfig),
		consumers:   newConsumers(consumerConfig),
		topicPrefix: prefix,
	}
}

func (c *Client) Start() (err error) {
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

func (c *Client) Pre(mw ...kafka.MiddlewareFunc) {
	for _, v := range mw {
		c.consumers.mwFuncs = append(c.consumers.mwFuncs, v)
	}
}

func (c *Client) StopSubscribe() {
	c.consumers.stopConsumers()
}

func (c *Client) StopProduce() {
	c.producer.stop()
}

func (c *Client) Publish(ctx context.Context, topic string, data interface{}, headers ...map[string][]byte) (err error) {
	publicationData, err := entity.NewPublicationData(ctx, data)
	if err != nil {
		return errors.Wrap(err, "cant get publication data")
	}
	return c.publishByte(ctx, topic, publicationData, headers...)
}

func (c *Client) publishByte(ctx context.Context, topic string, data kafka.PublicationData, headers ...map[string][]byte) (err error) {
	value, err := data.GetValue(ctx)
	if err != nil {
		return errors.Wrap(err, "cant get publication value")
	}
	message := kafka.NewMessage(topic, value, kafka.NewMessageHeaders(headers...), data.GetKey(ctx))
	message.Topic = c.topicPrefix + message.Topic
	message.Headers.SetHeader(serviceNameHeaderKey, []byte(c.serviceName))
	return c.producer.publish(ctx, message)
}

func (c *Client) Subscribe(h kafka.Handler, countConsumers int, specification kafka.Specifications) {
	log := logger.GetLogger()
	topicSpecification := kafka.NewTopicSpecifications(specification)
	topicSpecification.Topic = c.topicPrefix + topicSpecification.Topic
	for j := 0; j < countConsumers; j++ {
		err := c.consumers.addNewConsumer(h, topicSpecification)
		if err != nil {
			log.Fatal(err, "can't create new consumer")
		}
	}
}

func (c *Client) PrePublish(f kafka.Pre) {
	c.producer.prePublish = append(c.producer.prePublish, f)
}
