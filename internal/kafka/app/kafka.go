package app

import (
	"github.com/enkodio/pkg-kafka/internal/kafka/examples"
	configEntity "github.com/enkodio/pkg-kafka/internal/pkg/config/entity"
)

func Run(configSettings configEntity.Settings, serviceName string) {
	examples.RunWithKey(configSettings, serviceName)
	select {}
}
