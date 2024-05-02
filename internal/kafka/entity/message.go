package entity

import (
	cKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/enkodio/pkg-kafka/kafka"
)

func NewByKafkaMessage(message *cKafka.Message) kafka.Message {
	return kafka.Message{
		Headers: newByKafkaHeaders(message.Headers),
		Body:    message.Value,
		Topic:   *message.TopicPartition.Topic,
		Key:     message.Key,
	}
}

func newByKafkaHeaders(kafkaHeaders []cKafka.Header) kafka.MessageHeaders {
	var headers = make(kafka.MessageHeaders, len(kafkaHeaders))
	for i := 0; i < len(kafkaHeaders); i++ {
		headers[i] = kafka.MessageHeader{
			Key:   kafkaHeaders[i].Key,
			Value: kafkaHeaders[i].Value,
		}
	}
	return headers
}
