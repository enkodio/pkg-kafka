package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Message struct {
	Key     []byte         `json:"key"`
	Headers MessageHeaders `json:"headers"`
	Body    []byte         `json:"body"`
	Topic   string         `json:"topic"`
}

func NewMessage(topic string, body []byte, headers MessageHeaders, key []byte) Message {
	return Message{
		Topic:   topic,
		Body:    body,
		Headers: headers,
		Key:     key,
	}
}

func (m *Message) GetBody() []byte {
	return m.Body
}

func (m *Message) GetBodyAsString() string {
	return string(m.Body)
}

func (m *Message) ToKafkaMessage() *kafka.Message {
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &m.Topic,
			Partition: kafka.PartitionAny},
		Value:   m.Body,
		Headers: m.Headers.toKafkaHeaders(),
		Key:     m.Key,
	}
}

func (m *Message) SetHeader(key string, value []byte) {
	m.Headers.SetHeader(key, value)
}

func (m *Message) GetValueByKey(key string) []byte {
	return m.Headers.GetValueByKey(key)
}
