package client

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	serviceNameHeaderKey = "service_name"
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

func (m MessageHeader) GetKey() string {
	return m.Key
}

func (m MessageHeader) GetValue() []byte {
	return m.Value
}

type Headers []Header

func (h Headers) toKafkaHeaders() []kafka.Header {
	var headers = make([]kafka.Header, len(h))
	for i, header := range h {
		headers[i] = kafka.Header{
			Key:   header.GetKey(),
			Value: header.GetValue(),
		}
	}
	return headers
}

func (h *Headers) SetServiceName(serviceName string) {
	h.SetHeader(serviceNameHeaderKey, []byte(serviceName))
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

func (h Headers) GetValidHeaders() Headers {
	validHeaders := make([]Header, 0, len(h))
	for _, header := range h {
		if header == nil {
			continue
		}
		validHeaders = append(validHeaders, header)
	}
	return validHeaders
}

func (h Headers) GetServiceName() string {
	return string(h.GetValueByKey(serviceNameHeaderKey))
}
