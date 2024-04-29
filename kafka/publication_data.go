package kafka

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
)

type PublicationData struct {
	Value interface{}
	Key   string
}

func (p PublicationData) GetKey(_ context.Context) []byte {
	return []byte(p.Key)
}

func (p PublicationData) GetValue(_ context.Context) ([]byte, error) {
	switch p.Value.(type) {
	case string:
		return []byte(p.Value.(string)), nil
	case []byte:
		return p.Value.([]byte), nil
	default:
		value, err := json.Marshal(p.Value)
		if err != nil {
			return []byte(""), errors.Wrap(err, "cant marshal data")
		}
		return value, nil
	}
}

type PublicationDataWithKey interface {
	GetKey(context.Context) string
}

type PublicationDataWithValue interface {
	GetValue(context.Context) ([]byte, error)
}
