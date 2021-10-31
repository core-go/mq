package avro

import "github.com/hamba/avro"

type Marshaller struct {
	Schema avro.Schema
}

func NewMarshaller(schema avro.Schema) *Marshaller {
	return &Marshaller{Schema: schema}
}
func (c *Marshaller) Unmarshal(data []byte, v interface{}) error {
	return avro.Unmarshal(c.Schema, data, v)
}
func (c *Marshaller) Marshal(v interface{}) ([]byte, error) {
	return avro.Marshal(c.Schema, v)
}
