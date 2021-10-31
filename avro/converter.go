package avro

import (
	"context"
	"github.com/linkedin/goavro"
)

type Converter struct {
	Codec *goavro.Codec
	buf []byte
}
func NewConverter(codec *goavro.Codec, options...[]byte) *Converter {
	c := &Converter{Codec: codec}
	if len(options) > 0 {
		c.buf = options[0]
	}
	return c
}

func (c *Converter) ToBinary(ctx context.Context, data []byte) ([]byte, error) {
	native, _, err := c.Codec.NativeFromTextual(data)
	if err != nil {
		return data, err
	}
	return c.Codec.BinaryFromNative(c.buf, native)
}
func (c *Converter) FromBinary(ctx context.Context, data []byte) ([]byte, error) {
	native, _, err := c.Codec.NativeFromBinary(data)
	if err != nil {
		return data, err
	}
	return c.Codec.TextualFromNative(c.buf, native)
}
