package kafka

import (
	"context"
)

type (
	MessageHandler func(ctx context.Context, message Message) error
	MiddlewareFunc func(next MessageHandler) MessageHandler
)

type Handler func(ctx context.Context, message []byte) error
type Pre func(ctx context.Context, message *Message)

type Client interface {
	Start() error
	Pre(mw ...MiddlewareFunc)
	StopSubscribe()
	StopProduce()
	Publish(context.Context, string, interface{}, ...map[string][]byte) error
	Subscribe(Handler, int, Specifications)
	PrePublish(Pre)
}
