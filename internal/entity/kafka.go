package entity

import (
	"context"
)

type (
	MessageHandler      func(ctx context.Context, message Message) error
	MiddlewareFunc      func(next MessageHandler) MessageHandler
	MiddlewareFunctions []MiddlewareFunc
)

type Handler func(ctx context.Context, message []byte) error

type TopicSpecifications struct {
	Topic             string
	NumPartitions     int
	ReplicationFactor int
	CheckError        bool
}

type BrokerClient interface {
	Start() error
	Pre(mw ...MiddlewareFunc)
	StopSubscribe()
	StopProduce()
	Publish(context.Context, Message) error
	Subscribe(Handler, int, TopicSpecifications)
}
