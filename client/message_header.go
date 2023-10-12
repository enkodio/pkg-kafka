package client

import "github.com/confluentinc/confluent-kafka-go/kafka"

type MessageHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func (m MessageHeader) GetKey() string {
	return m.Key
}

func (m MessageHeader) GetValue() []byte {
	return m.Value
}

type MessageHeaders []MessageHeader

func NewByKafkaHeaders(kafkaHeaders []kafka.Header) MessageHeaders {
	var headers = make(MessageHeaders, len(kafkaHeaders))
	for i := 0; i < len(kafkaHeaders); i++ {
		headers[i] = MessageHeader{
			Key:   kafkaHeaders[i].Key,
			Value: kafkaHeaders[i].Value,
		}
	}
	return headers
}

func NewMessageHeaders(headers Headers) MessageHeaders {
	messageHeaders := make(MessageHeaders, 0, len(headers))
	for _, h := range headers {
		if h == nil {
			continue
		}
		messageHeaders = append(messageHeaders, MessageHeader{
			Key:   h.GetKey(),
			Value: h.GetValue(),
		})
	}
	return messageHeaders
}

func (m MessageHeaders) ToHeaders() Headers {
	headers := make(Headers, len(m))
	for i, h := range m {
		headers[i] = h
	}
	return headers
}

func (m MessageHeaders) GetValueByKey(key string) []byte {
	for _, header := range m {
		if header.GetKey() == key {
			return header.GetValue()
		}
	}
	return nil
}

func (m *MessageHeaders) SetHeader(key string, value []byte) {
	newHeader := MessageHeaders{MessageHeader{
		Key:   key,
		Value: value,
	}}
	*m = append(newHeader, *m...)
}

func (m MessageHeaders) toKafkaHeaders() []kafka.Header {
	var headers = make([]kafka.Header, len(m))
	for i, header := range m {
		headers[i] = kafka.Header{
			Key:   header.GetKey(),
			Value: header.GetValue(),
		}
	}
	return headers
}

func (m *MessageHeaders) setServiceName(serviceName string) {
	m.SetHeader(serviceNameHeaderKey, []byte(serviceName))
}
