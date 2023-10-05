package kafka

import (
	"context"
	"gitlab.enkod.tech/pkg/kafka/internal/entity"
)

type (
	MessageHandler func(ctx context.Context, message entity.CustomMessage) error
	MiddlewareFunc func(next MessageHandler) MessageHandler
)

type Handler func(ctx context.Context, message []byte) error
type Pre func(ctx context.Context, message *entity.Message)

type Client interface {
	Start() error
	Pre(mw ...MiddlewareFunc)
	StopSubscribe()
	StopProduce()
	Publish(context.Context, string, interface{}, ...entity.Header) error
	Subscribe(Handler, int, entity.Specifications)
	PrePublish(Pre)
}
