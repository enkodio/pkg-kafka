package logger

import (
	"context"

	"github.com/sirupsen/logrus"
)

type contextKey int

const (
	ctxLoggerKey contextKey = iota
)

func FromContext(ctx context.Context) logrus.FieldLogger {
	if ctx == nil {
		return logger
	}
	if ctxLogger, ok := ctx.Value(ctxLoggerKey).(logrus.FieldLogger); ok {
		return ctxLogger
	}
	return logger
}
