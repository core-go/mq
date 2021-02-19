package mq

import (
	"context"
	"encoding/json"
)

type MessageQueueWriter struct {
	Produce func(ctx context.Context, data []byte, attributes map[string]string) (string, error)
}

func NewMessageQueueWriter(produce func(context.Context, []byte, map[string]string) (string, error)) *MessageQueueWriter {
	return &MessageQueueWriter{produce}
}

func (w *MessageQueueWriter) Write(ctx context.Context, model interface{}) error {
	if model == nil {
		return nil
	}
	data, er1 := Marshal(model)
	if er1 != nil {
		return er1
	}
	msg := GetMessageFromContext(ctx)
	if msg != nil && len(msg.Attributes) > 0 {
		_, er2 := w.Produce(ctx, data, msg.Attributes)
		return er2
	} else {
		_, er2 := w.Produce(ctx, data, nil)
		return er2
	}
}
func Marshal(v interface{}) ([]byte, error) {
	b, ok1 := v.([]byte)
	if ok1 {
		return b, nil
	}
	s, ok2 := v.(string)
	if ok2 {
		return []byte(s), nil
	}
	return json.Marshal(v)
}
func GetMessageFromContext(ctx context.Context) *Message {
	msg := ctx.Value("message")
	if msg != nil {
		k, ok := msg.(*Message)
		if ok {
			return k
		}
	}
	return nil
}
