package entity

import "github.com/confluentinc/confluent-kafka-go/kafka"

type MessageHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type Message struct {
	Key     string          `json:"key"`
	Headers []MessageHeader `json:"headers"`
	Body    []byte          `json:"body"`
	Topic   string          `json:"topic"`
}

func NewByKafkaMessage(message *kafka.Message) Message {
	var headers = make([]MessageHeader, len(message.Headers))
	for i := 0; i < len(message.Headers); i++ {
		headers[i] = MessageHeader{
			Key:   message.Headers[i].Key,
			Value: message.Headers[i].Value,
		}
	}

	return Message{
		Headers: headers,
		Body:    message.Value,
		Topic:   *message.TopicPartition.Topic,
	}
}

func (m *Message) ToKafkaMessage() *kafka.Message {
	var headers = make([]kafka.Header, len(m.Headers))
	for i := 0; i < len(m.Headers); i++ {
		headers[i] = kafka.Header{
			Key:   m.Headers[i].Key,
			Value: m.Headers[i].Value,
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
