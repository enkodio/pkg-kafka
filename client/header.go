package client

const (
	serviceNameHeaderKey = "service_name"
)

type Header interface {
	GetKey() string
	GetValue() []byte
}

type Headers []Header

func (h Headers) GetValueByKey(key string) []byte {
	for _, header := range h {
		if header.GetKey() == key {
			return header.GetValue()
		}
	}
	return nil
}

func (h *Headers) SetHeader(key string, value []byte) {
	*h = append(*h, &MessageHeader{
		Key:   key,
		Value: value,
	})
}
