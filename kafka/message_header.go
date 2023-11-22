package kafka

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

func NewMessageHeaders(headers ...map[string][]byte) MessageHeaders {
	messageHeaders := make(MessageHeaders, 0, len(headers))
	for _, h := range headers {
		for key, value := range h {
			messageHeaders = append(messageHeaders, MessageHeader{
				Key:   key,
				Value: value,
			})
		}
	}
	return messageHeaders
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
	*m = append(*m, MessageHeader{
		Key:   key,
		Value: value,
	})
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
