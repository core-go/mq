package mq

import (
	"context"
	"github.com/pkg/errors"
)

type BytesWriter struct {
	Send func(ctx context.Context, data []byte, attributes map[string]string) (string, error)
}
func NewBytesWriter(send func(ctx context.Context, data []byte, attributes map[string]string) (string, error)) *BytesWriter {
	return &BytesWriter{Send: send}
}
func (w *BytesWriter) Write(ctx context.Context, data interface{}) error {
	d, ok := data.([]byte)
	if !ok {
		return errors.New("data must be byte array ([]byte)")
	} else {
		_, err := w.Send(ctx, d, nil)
		return err
	}
}
