package entity

import (
	"context"
)

type (
	MessageHandler func(ctx context.Context, message CustomMessage) error
	MiddlewareFunc func(next MessageHandler) MessageHandler
)

type Handler func(ctx context.Context, message []byte) error

type BrokerClient interface {
	Start() error
	Pre(mw ...MiddlewareFunc)
	StopSubscribe()
	StopProduce()
	Publish(context.Context, CustomMessage) error
	Subscribe(Handler, int, Specifications)
}
