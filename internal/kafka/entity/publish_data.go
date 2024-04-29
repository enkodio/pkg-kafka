package entity

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
)

type publicationDataWithKey interface {
	GetKey(context.Context) string
}

type publicationDataWithValue interface {
	GetValue(context.Context) ([]byte, error)
}

type PublicationData struct {
	Value []byte
	Key   []byte
}

func NewPublishData(ctx context.Context, data interface{}) (PublicationData, error) {
	if dataS, ok := data.(string); ok {
		return PublicationData{
			Value: []byte(dataS),
		}, nil
	}

	var err error
	p := PublicationData{}
	if withValueData, ok := data.(publicationDataWithValue); ok {
		p.Value, err = withValueData.GetValue(ctx)
		if err != nil {
			return PublicationData{}, err
		}
	} else {
		p.Value, err = json.Marshal(data)
		if err != nil {
			return PublicationData{}, errors.Wrap(err, "cant marshal data")
		}
	}

	if withKeyData, ok := data.(publicationDataWithKey); ok {
		p.Key = []byte(withKeyData.GetKey(ctx))
	}
	return p, nil
}
