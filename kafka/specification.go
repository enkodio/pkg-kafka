package kafka

type Specifications interface {
	GetTopic() string
	GetNumPartitions() int
	GetReplicationFactor() int
	GetCheckError() bool
}
