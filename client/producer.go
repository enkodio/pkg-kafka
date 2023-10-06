package client

import (
	"context"
	"github.com/CossackPyra/pyraconv"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"gitlab.enkod.tech/pkg/kafka/internal/entity"
	"gitlab.enkod.tech/pkg/kafka/pkg/logger"
	"time"
)

type producer struct {
	config        kafka.ConfigMap
	kafkaProducer *kafka.Producer
	syncGroup     *entity.SyncGroup
	prePublish    []Pre
}

func newProducer(config kafka.ConfigMap) *producer {
	config["client.id"] = uuid.New().String()

	// FIXME Два костыля, нужно подумать, что делать с тем, что с консула числа маршлятся во float64
	config["queue.buffering.max.messages"] = int(pyraconv.ToInt64(config["queue.buffering.max.messages"]))
	config["linger.ms"] = int(pyraconv.ToInt64(config["linger.ms"]))
	return &producer{
		config:    config,
		syncGroup: entity.NewSyncGroup(),
	}
}

func (p *producer) initProducer() (err error) {
	log := logger.GetLogger()
	p.kafkaProducer, err = kafka.NewProducer(&p.config)
	if err != nil {
		return errors.Wrap(err, "cant create kafka producer")
	}
	log.Info("KAFKA PRODUCER IS READY")
	return nil
}

func (p *producer) stop() {
	p.syncGroup.Close()
	p.kafkaProducer.Flush(flushTimeout)
	p.kafkaProducer.Close()
}

func (p *producer) produce(ctx context.Context, message *kafka.Message, deliveryChannel chan kafka.Event) error {
	log := logger.FromContext(ctx)
	for {
		err := p.kafkaProducer.Produce(message, deliveryChannel)
		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrQueueFull {
				// Если очередь забита, пробуем отправить снова через 5 секунд
				log.WithError(err).
					Warnf("kafka queue full, try again after %v second", queueFullWaitTime.Seconds())
				time.Sleep(queueFullWaitTime)
				continue
			} else {
				return err
			}
		}
		break
	}
	return nil
}

func (p *producer) createTopics(topics []TopicSpecifications) (err error) {
	// Создаём админский клиент через настройки подключения продусера
	adminClient, err := kafka.NewAdminClientFromProducer(p.kafkaProducer)
	if err != nil {
		return errors.Wrap(err, "cant init kafka admin client")
	}
	defer adminClient.Close()
	log := logger.GetLogger()
	specifications := make([]kafka.TopicSpecification, 0, len(topics))
	for _, topic := range topics {
		specification := kafka.TopicSpecification{
			Topic:             topic.Topic,
			ReplicationFactor: topic.GetReplicationFactor(),
			NumPartitions:     topic.GetNumPartitions(),
		}
		specifications = append(specifications, specification)
	}
	result, err := adminClient.CreateTopics(context.Background(), specifications)
	if err != nil {
		return errors.Wrapf(err, "%v: cant create topics", err.Error())
	}
	for _, v := range result {
		if kafkaErr, ok := errToKafka(v.Error); ok {
			if kafkaErr.Code() == kafka.ErrTopicAlreadyExists {
				continue
			}
		}
		// Если такой топик уже есть, то будет ошибка внутри структуры, если ошибки нет, то в структуре будет "Success"
		log.Infof("%v: %v", v.Topic, v.Error.String())
	}
	return nil
}

func (p *producer) publish(ctx context.Context, message Message) (err error) {
	if p.syncGroup.IsClosed() {
		return errors.New("producer was closed")
	}
	deliveryChannel := make(chan kafka.Event)
	p.syncGroup.Add(1)
	for _, pre := range p.prePublish {
		pre(ctx, &message)
	}
	go p.handleDelivery(ctx, message, deliveryChannel)

	err = p.produce(
		ctx,
		message.ToKafkaMessage(),
		deliveryChannel,
	)
	if err != nil {
		return err
	}
	return
}

func (p *producer) handleDelivery(ctx context.Context, message Message, deliveryChannel chan kafka.Event) {
	defer p.syncGroup.Done()
	log := logger.FromContext(ctx)
	e := <-deliveryChannel
	close(deliveryChannel)
	switch event := e.(type) {
	case *kafka.Message:
		if kafkaErr, ok := errToKafka(event.TopicPartition.Error); ok {
			// Если retriable, то ошибка временная, нужно пытаться переотправить снова, если нет, то ошибка nonretriable, просто логируем
			if kafkaErr.IsRetriable() {
				log.WithError(kafkaErr).
					Errorf("kafka produce retriable error, try again send topic: %v, message: %v",
						message.Topic, message.GetBodyAsString())
				err := p.publish(ctx, message)
				if err != nil {
					log.WithError(err).
						Errorf("Cant publish by kafka, topic: %v, message: %v",
							message.Topic, message.GetBodyAsString())
				}
			} else {
				log.WithError(kafkaErr).
					Errorf("kafka produce nonretriable error, can't send topic: %v, message: %v. Is fatal: %v",
						message.Topic, message.GetBodyAsString(), kafkaErr.IsFatal())
			}
		}
	case kafka.Error:
		// Общие пользовательские ошибки, клиент сам пытается переотправить, просто логируем
		log.WithError(event).
			Errorf("publish error, topic: %v, message: %v. client tries to send again", message.Topic, message.GetBodyAsString())
	}
}
