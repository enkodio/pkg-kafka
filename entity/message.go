package entity

import "github.com/confluentinc/confluent-kafka-go/kafka"

type CustomMessage interface {
	GetKey() string
	GetHeaders() []Header
	GetBody() []byte
	GetTopic() string
}

type Message struct {
	Key     string   `json:"key"`
	Headers []Header `json:"headers"`
	Body    []byte   `json:"body"`
	Topic   string   `json:"topic"`
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
	var headers = make([]Header, len(message.Headers))
	for i := 0; i < len(message.Headers); i++ {
		headers[i] = &MessageHeader{
			Key:   message.Headers[i].Key,
			Value: message.Headers[i].Value,
		}
	}

	return &Message{
		Headers: headers,
		Body:    message.Value,
		Topic:   *message.TopicPartition.Topic,
	}
}

func (m *Message) GetKey() string {
	return m.Key
}

func (m *Message) GetHeaders() []Header {
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
	var headers = make([]kafka.Header, len(m.Headers))
	for i := 0; i < len(m.Headers); i++ {
		headers[i] = kafka.Header{
			Key:   m.Headers[i].GetKey(),
			Value: m.Headers[i].GetValue(),
		}
	}

	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &m.Topic,
			Partition: kafka.PartitionAny},
		Value:   m.Body,
		Headers: headers,
	}
}
