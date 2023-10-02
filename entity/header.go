package entity

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	ServiceNameHeaderKey = "service_name"
)

type Header interface {
	GetKey() string
	GetValue() []byte
}

type MessageHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func NewByKafkaHeaders(kafkaHeaders []kafka.Header) Headers {
	var headers = make([]Header, len(kafkaHeaders))
	for i := 0; i < len(kafkaHeaders); i++ {
		headers[i] = &MessageHeader{
			Key:   kafkaHeaders[i].Key,
			Value: kafkaHeaders[i].Value,
		}
	}
	return headers
}

func (m *MessageHeader) GetKey() string {
	return m.Key
}

func (m *MessageHeader) GetValue() []byte {
	return m.Value
}

type Headers []Header

func (h Headers) ToKafkaHeaders() []kafka.Header {
	var headers = make([]kafka.Header, len(h))
	for i := 0; i < len(h); i++ {
		headers[i] = kafka.Header{
			Key:   h[i].GetKey(),
			Value: h[i].GetValue(),
		}
	}
	return headers
}

func (h Headers) GetValueByKey(key string) []byte {
	for _, header := range h {
		if header.GetKey() == key {
			return header.GetValue()
		}
	}
	return nil
}

func (h *Headers) SetHeader(key string, value []byte) {
	*h = append(*h, &MessageHeader{
		Key:   key,
		Value: value,
	})
}
