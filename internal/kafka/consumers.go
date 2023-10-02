package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"kafka_client/internal/entity"
	"kafka_client/pkg/logger"
	"sync"
	"time"
)

type consumers struct {
	config    kafka.ConfigMap
	consumers []*consumer
	mwFuncs   []entity.MiddlewareFunc
	syncGroup *entity.SyncGroup
}

func newConsumers(config kafka.ConfigMap) consumers {
	return consumers{
		config:    config,
		consumers: make([]*consumer, 0),
		syncGroup: entity.NewSyncGroup(),
	}
}

func (c *consumers) getUniqByNameTopicSpecifications() []entity.TopicSpecifications {
	topicsMap := make(map[string]struct{}, len(c.consumers))
	topics := make([]entity.TopicSpecifications, 0, len(c.consumers))

	for _, consumer := range c.consumers {
		if _, ok := topicsMap[consumer.Topic]; ok {
			continue
		}
		topicsMap[consumer.Topic] = struct{}{}
		topics = append(topics, consumer.TopicSpecifications)
	}
	return topics
}

func (c *consumers) addNewConsumer(handler entity.Handler, topicSpecification entity.TopicSpecifications) error {
	newConsumer := newConsumer(topicSpecification, handler)
	err := newConsumer.initConsumer(c.config)
	if err != nil {
		return errors.Wrap(err, "cant init kafka consumer")
	}
	c.consumers = append(c.consumers, newConsumer)
	return nil
}

func (c *consumers) createKafkaConsumers() error {
	for _, consumer := range c.consumers {
		err := consumer.initConsumer(c.config)
		if err != nil {
			return errors.Wrap(err, "cant init kafka consumer")
		}
	}
	return nil
}

func (c *consumers) stopConsumers() {
	log := logger.GetLogger()
	c.syncGroup.Close()

	for i := range c.consumers {
		_, err := c.consumers[i].Commit()
		if kafkaErr, ok := errToKafka(err); ok {
			if kafkaErr.Code() != kafka.ErrNoOffset {
				log.WithError(err).Errorf("cant commit offset for topic: %s", err.Error())
			}
		}
		// Отписка от назначенных топиков
		err = c.consumers[i].Unsubscribe()
		if err != nil {
			log.WithError(err).Errorf("cant unsubscribe connection: %s", err.Error())
		}
		// Закрытие соединения
		err = c.consumers[i].Close()
		if err != nil {
			log.WithError(err).Errorf("cant close consumer connection: %s", err.Error())
		}
	}
}

func (c *consumers) initConsumers() {
	once := &sync.Once{}
	// Запускаем каждого консумера в отдельной горутине
	for _, cns := range c.consumers {
		c.syncGroup.Add(1)
		go func(consumer *consumer, syncGroup *entity.SyncGroup) {
			err := consumer.startConsume(syncGroup, c.mwFuncs)
			c.syncGroup.Done()
			if err != nil {
				once.Do(func() {
					c.reconnect()
				})
			}
		}(cns, c.syncGroup)
	}
	c.syncGroup.Start()
	logger.GetLogger().Info("KAFKA CONSUMERS IS READY")
	return
}

func (c *consumers) reconnect() {
	log := logger.GetLogger()
	log.Debugf("start reconnecting consumers")
	// Стопаем консумеры
	c.stopConsumers()
	log.Debugf("consumers stopped")

	// Ждём 10 секунд для реконнекта
	time.Sleep(reconnectTime)

	// Запускаем новые консумеры
	for {
		err := c.createKafkaConsumers()
		if err != nil {
			logger.FromContext(nil).WithError(err).Error("cant init consumers")
			time.Sleep(reconnectTime)
			continue
		}
		log.Debugf("new consumers created")
		break
	}

	c.initConsumers()
}
