package logic

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

const (
	serviceNameHeaderKey = "service_name"
)

func errToKafka(err error) (kafka.Error, bool) {
	var kafkaErr kafka.Error
	if err == nil {
		return kafkaErr, false
	}
	errors.As(err, &kafkaErr)
	return kafkaErr, true
}
