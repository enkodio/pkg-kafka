package client

import (
	"context"
	cKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/enkodio/pkg-kafka/internal/entity"
	"github.com/enkodio/pkg-kafka/internal/pkg/logger"
	"github.com/enkodio/pkg-kafka/kafka"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type consumer struct {
	handler kafka.Handler
	TopicSpecifications
	*cKafka.Consumer
}

func newConsumer(
	topicSpecifications TopicSpecifications,
	handler kafka.Handler,
) *consumer {
	return &consumer{
		TopicSpecifications: topicSpecifications,
		handler:             handler,
	}
}

func (c *consumer) initConsumer(config cKafka.ConfigMap) error {
	config["client.id"] = uuid.New().String()
	// Создаём консумера
	kafkaConsumer, err := cKafka.NewConsumer(&config)
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

func (c *consumer) getRebalanceCb() cKafka.RebalanceCb {
	return func(c *cKafka.Consumer, event cKafka.Event) error {
		logger.GetLogger().Debugf("Rebalanced: %v; rebalanced protocol: %v;",
			event.String(),
			c.GetRebalanceProtocol())
		return nil
	}
}

func (c *consumer) startConsume(syncGroup *entity.SyncGroup, mwFuncs []kafka.MiddlewareFunc) error {
	log := logger.GetLogger()
	// Прогоняем хендлер через миддлверы
	var handler kafka.MessageHandler = func(ctx context.Context, message Message) error {
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
				if kafkaErr.Code() == cKafka.ErrTimedOut || kafkaErr.IsRetriable() {
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

func (c *consumer) rollbackConsumerTransaction(topicPartition cKafka.TopicPartition) {
	// В committed лежит массив из одного элемента, потому что передаём одну партицию, которую нужно сбросить
	committed, err := c.Committed([]cKafka.TopicPartition{{Topic: &c.Topic, Partition: topicPartition.Partition}}, -1)
	log := logger.GetLogger()
	if err != nil {
		log.Error(err)
		return
	}
	if committed[0].Offset < 0 {
		committed[0].Offset = cKafka.OffsetBeginning
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
