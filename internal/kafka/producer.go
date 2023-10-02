package kafka

import (
	"context"
	"github.com/CossackPyra/pyraconv"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"kafka_client/internal/entity"
	"kafka_client/pkg/logger"
	"time"
)

type producer struct {
	config        kafka.ConfigMap
	kafkaProducer *kafka.Producer
}

func newProducer(config kafka.ConfigMap) *producer {
	return &producer{
		config: config,
	}
}

func (p *producer) initProducer() (err error) {
	log := logger.GetLogger()
	p.config["client.id"] = uuid.New().String()

	// FIXME Два костыля, нужно подумать, что делать с тем, что с консула числа маршлятся во float64
	p.config["queue.buffering.max.messages"] = int(pyraconv.ToInt64(p.config["queue.buffering.max.messages"]))
	p.config["linger.ms"] = int(pyraconv.ToInt64(p.config["linger.ms"]))

	p.kafkaProducer, err = kafka.NewProducer(&p.config)
	if err != nil {
		return errors.Wrap(err, "cant create kafka producer")
	}
	log.Info("KAFKA PRODUCER IS READY")
	return nil
}

func (p *producer) stop() {
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

func (p *producer) createTopics(topics []entity.TopicSpecifications) (err error) {
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
			ReplicationFactor: defaultReplicationFactor,
			NumPartitions:     defaultNumPartitions,
		}
		// Если нет настроек топика, то при создании будут подставляться дефолтные
		if topic.ReplicationFactor != 0 {
			if topic.ReplicationFactor > maxReplicationFactor {
				topic.ReplicationFactor = maxReplicationFactor
			}
			specification.ReplicationFactor = topic.ReplicationFactor
		}
		if topic.NumPartitions > 0 {
			specification.NumPartitions = topic.NumPartitions
		}
		specifications = append(specifications, specification)
	}
	result, err := adminClient.CreateTopics(context.Background(), specifications)
	if err != nil {
		return errors.Wrapf(err, "%v: cant create topics", err.Error())
	}
	for _, v := range result {
		// Если такой топик уже есть, то будет ошибка внутри структуры, если ошибки нет, то в структуре будет "Success"
		log.Infof("%v: %v", v.Topic, v.Error.String())
	}
	return nil
}

func (p *producer) publish(ctx context.Context, message entity.Message) (err error) {
	deliveryChannel := make(chan kafka.Event)

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

func (p *producer) handleDelivery(ctx context.Context, message entity.Message, deliveryChannel chan kafka.Event) {
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
						message.Topic, string(message.Body))
				err := p.publish(ctx, message)
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
