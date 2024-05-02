package examples

import (
	"context"
	"encoding/json"
)

type publishingData struct {
	Value interface{} `json:"value"`
	Key   string      `json:"-"`
}

func (p publishingData) GetKey(context.Context) string {
	return p.Key
}

func (p publishingData) GetValue(context.Context) ([]byte, error) {
	if s, ok := p.Value.(string); ok {
		return []byte(s), nil
	}
	return json.Marshal(p.Value)
}
