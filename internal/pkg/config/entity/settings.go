package entity

import "github.com/confluentinc/confluent-kafka-go/kafka"

type Settings struct {
	KafkaProducer kafka.ConfigMap `json:"kafkaProducer"`
	KafkaConsumer kafka.ConfigMap `json:"kafkaConsumer"`
}
