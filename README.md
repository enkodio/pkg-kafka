# Package for work with kafka client library confluent-kafka-go.

Getting Started
===============

Manual install:
```bash
go get -u github.com/enkodio/pkg-kafka
```

Golang import:
```golang
import "github.com/enkodio/pkg-kafka/client"
```

If you are building for Alpine Linux (musl), `-tags musl` must be specified.

```bash
go build -tags musl ./...
```

Examples
========
```golang
import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	kafkaClient "github.com/enkodio/pkg-kafka/client"
)

func main() {
	const (
		testTopic = "topic"
	)
	var (
		k = kafkaClient.NewClient(
			kafka.ConfigMap{
				"bootstrap.servers": "localhost",
				"group.id":          "myGroup",
				"auto.offset.reset": "earliest",
			},
			kafka.ConfigMap{
				"bootstrap.servers": "localhost",
			},
			"example_service",
			nil,
			"")
	)
	k.Subscribe(handler, 1, &kafkaClient.TopicSpecifications{
		NumPartitions:     1,
		ReplicationFactor: 1,
		Topic:             testTopic,
	})

	err := k.Publish(context.Background(), testTopic, "body", map[string][]byte{
		"key": []byte("value"),
	})
	if err != nil {
		panic(err)
	}
}

func handler(ctx context.Context, msg []byte) error {
	fmt.Println(string(msg))
	return nil
}
```