package examples

import (
	"context"
	"github.com/enkodio/pkg-kafka/internal/pkg/logger"
	"github.com/enkodio/pkg-kafka/kafka"
)

func getTestMiddleware() kafka.MiddlewareFunc {
	return func(next kafka.MessageHandler) kafka.MessageHandler {
		return func(ctx context.Context, message kafka.Message) error {
			logger.GetLogger().Info("got middleware")
			return next(ctx, message)
		}
	}
}
