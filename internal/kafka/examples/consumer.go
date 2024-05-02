package examples

import (
	"context"
	"github.com/enkodio/pkg-kafka/internal/pkg/logger"
	"github.com/enkodio/pkg-kafka/kafka"
)

func testConsumer(k kafka.Client, handler kafka.Handler, specifications kafka.TopicSpecifications) {
	k.Subscribe(handler, 1, &specifications)
}

func testHandler(name string) func(ctx context.Context, msg []byte) error {
	log := logger.GetLogger()
	return func(ctx context.Context, msg []byte) error {
		log.Infof("handle message in consumer :%v; message : %v", name, string(msg))
		return nil
	}
}
