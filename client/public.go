package client

import (
	"context"
)

type (
	MessageHandler func(ctx context.Context, message CustomMessage) error
	MiddlewareFunc func(next MessageHandler) MessageHandler
)

type Handler func(ctx context.Context, message []byte) error
type Pre func(ctx context.Context, message *Message)

type Client interface {
	Start() error
	Pre(mw ...MiddlewareFunc)
	StopSubscribe()
	StopProduce()
	Publish(context.Context, string, interface{}, ...Header) error
	PublishByte(context.Context, string, []byte, ...Header) (err error)
	Subscribe(Handler, int, Specifications)
	PrePublish(Pre)
}