package mq

import "context"

type BytesWriter struct {
	Send func(ctx context.Context, data []byte, attributes map[string]string) (string, error)
}
func NewBytesWriter(send func(ctx context.Context, data []byte, attributes map[string]string) (string, error)) *BytesWriter {
	return &BytesWriter{Send: send}
}
func (w *BytesWriter) Write(ctx context.Context, model interface{}) error {
	data := model.([]byte)
	_, err := w.Send(ctx, data, nil)
	return err
}
