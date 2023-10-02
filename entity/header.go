package entity

type Header interface {
	GetKey() string
	GetValue() []byte
}

type MessageHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func (m *MessageHeader) GetKey() string {
	return m.Key
}

func (m *MessageHeader) GetValue() []byte {
	return m.Value
}
