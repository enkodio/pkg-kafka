package client

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"gitlab.enkod.tech/pkg/kafka/internal/entity"
	"gitlab.enkod.tech/pkg/kafka/pkg/logger"
)

type consumer struct {
	handler Handler
	TopicSpecifications
	*kafka.Consumer
}

func newConsumer(
	topicSpecifications TopicSpecifications,
	handler Handler,
) *consumer {
	return &consumer{
		TopicSpecifications: topicSpecifications,
		handler:             handler,
	}
}

func (c *consumer) initConsumer(config kafka.ConfigMap) error {
	config["client.id"] = uuid.New().String()
	// Создаём консумера
	kafkaConsumer, err := kafka.NewConsumer(&config)
	if err != nil {
		return errors.Wrap(err, "cant create kafka consumer")
	}
	// Подписываем консумера на топик
	err = kafkaConsumer.Subscribe(c.Topic, c.getRebalanceCb())
	if err != nil {
		return errors.Wrap(err, "cant subscribe kafka consumer")
	}
	c.Consumer = kafkaConsumer
	return nil
}

func (c *consumer) getRebalanceCb() kafka.RebalanceCb {
	return func(c *kafka.Consumer, event kafka.Event) error {
		logger.GetLogger().Debugf("Rebalanced: %v; rebalanced protocol: %v;",
			event.String(),
			c.GetRebalanceProtocol())
		return nil
	}
}

func (c *consumer) startConsume(syncGroup *entity.SyncGroup, mwFuncs []MiddlewareFunc) error {
	log := logger.GetLogger()
	// Прогоняем хендлер через миддлверы
	var handler MessageHandler = func(ctx context.Context, message Message) error {
		return c.handler(ctx, message.GetBody())
	}
	for j := len(mwFuncs) - 1; j >= 0; j-- {
		handler = mwFuncs[j](handler)
	}
	for {
		select {
		case <-syncGroup.IsDone():
			return nil
		default:
			msg, err := c.ReadMessage(readTimeout)
			if kafkaErr, ok := errToKafka(err); ok {
				// Если retriable (но со стороны консумера вроде бы такого нет), то пробуем снова
				if kafkaErr.Code() == kafka.ErrTimedOut || kafkaErr.IsRetriable() {
					continue
				}
				return errors.Wrap(err, "cant read kafka message")
			}
			err = handler(context.Background(), NewByKafkaMessage(msg))
			if err != nil && c.CheckError {
				log.WithError(err).Debug("try to read message again")
				c.rollbackConsumerTransaction(msg.TopicPartition)
			}
		}
	}
}

func (c *consumer) rollbackConsumerTransaction(topicPartition kafka.TopicPartition) {
	// В committed лежит массив из одного элемента, потому что передаём одну партицию, которую нужно сбросить
	committed, err := c.Committed([]kafka.TopicPartition{{Topic: &c.Topic, Partition: topicPartition.Partition}}, -1)
	log := logger.GetLogger()
	if err != nil {
		log.Error(err)
		return
	}
	if committed[0].Offset < 0 {
		committed[0].Offset = kafka.OffsetBeginning
	} else {
		committed[0].Offset = topicPartition.Offset
	}
	err = c.Seek(committed[0], 0)
	if err != nil {
		log.Error(err)
		return
	}
	return
}
