package entity

import "github.com/confluentinc/confluent-kafka-go/kafka"

type CustomMessage interface {
	GetKey() string
	GetHeaders() Headers
	GetBody() []byte
	GetTopic() string
}

type Message struct {
	Key     string  `json:"key"`
	Headers Headers `json:"headers"`
	Body    []byte  `json:"body"`
	Topic   string  `json:"topic"`
}

func NewMessage(message CustomMessage) Message {
	return Message{
		Key:     message.GetKey(),
		Headers: message.GetHeaders(),
		Body:    message.GetBody(),
		Topic:   message.GetTopic(),
	}
}

func NewByKafkaMessage(message *kafka.Message) CustomMessage {
	return &Message{
		Headers: NewByKafkaHeaders(message.Headers),
		Body:    message.Value,
		Topic:   *message.TopicPartition.Topic,
	}
}

func (m *Message) GetKey() string {
	return m.Key
}

func (m *Message) GetHeaders() Headers {
	return m.Headers
}

func (m *Message) GetBody() []byte {
	return m.Body
}

func (m *Message) GetTopic() string {
	return m.Topic
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
		Headers: m.Headers.ToKafkaHeaders(),
	}
}
