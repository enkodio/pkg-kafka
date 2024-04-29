package entity

import (
	"context"
	"encoding/json"
	"github.com/enkodio/pkg-kafka/kafka"
	"github.com/pkg/errors"
)

func NewPublicationData(ctx context.Context, data interface{}) (kafka.PublicationData, error) {
	switch data.(type) {
	case kafka.PublicationData:
		return data.(kafka.PublicationData), nil
	case string:
		return kafka.PublicationData{
			Value: []byte(data.(string)),
		}, nil
	case []byte:
		return kafka.PublicationData{
			Value: data.([]byte),
		}, nil
	}

	var err error
	p := kafka.PublicationData{}
	if withValueData, ok := data.(kafka.PublicationDataWithValue); ok {
		p.Value, err = withValueData.GetValue(ctx)
		if err != nil {
			return kafka.PublicationData{}, err
		}
	} else {
		p.Value, err = json.Marshal(data)
		if err != nil {
			return kafka.PublicationData{}, errors.Wrap(err, "cant marshal data")
		}
	}

	if withKeyData, ok := data.(kafka.PublicationDataWithKey); ok {
		p.Key = withKeyData.GetKey(ctx)
	}
	return p, nil
}
