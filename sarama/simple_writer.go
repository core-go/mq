package kafka

import (
	"context"
	"encoding/json"
	"github.com/Shopify/sarama"
)

type SimpleWriter struct {
	SyncProducer sarama.SyncProducer
	Convert      func(context.Context, []byte) ([]byte, error)
	Generate     func() string
}

func NewSimpleWriter(writer sarama.SyncProducer, convert func(context.Context, []byte)([]byte, error), options ...func() string) (*SimpleWriter, error) {
	var generate func() string
	if len(options) > 0 {
		generate = options[0]
	}
	return &SimpleWriter{SyncProducer: writer, Convert: convert, Generate: generate}, nil
}
func NewSimpleWriterByConfig(c WriterConfig, convert func(context.Context, []byte)([]byte, error), options ...func() string) (*SimpleWriter, error) {
	writer, err := newSyncProducer(c)
	if err != nil {
		return nil, err
	}
	return NewSimpleWriter(*writer, convert, options...)
}
func (p *SimpleWriter) Write(ctx context.Context, topic string, data []byte, messageAttributes map[string]string) (string, error) {
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, data)
		if err != nil {
			return "", err
		}
	}
	msg := sarama.ProducerMessage{Value: sarama.ByteEncoder(binary), Topic: topic}
	if messageAttributes != nil {
		msg.Headers = MapToHeader(messageAttributes)
	}
	if p.Generate != nil {
		id := p.Generate()
		msg.Key = sarama.StringEncoder(id)
		p, o, err := p.SyncProducer.SendMessage(&msg)
		m := make(map[string]interface{})
		m[Key] = id
		m[Partition] = p
		m[Offset] = o
		b, _ := json.Marshal(m)
		return string(b), err
	} else {
		p, o, err := p.SyncProducer.SendMessage(&msg)
		m := make(map[string]interface{})
		m[Partition] = p
		m[Offset] = o
		b, _ := json.Marshal(m)
		return string(b), err
	}
}
func (p *SimpleWriter) WriteWithKey(ctx context.Context, topic string, data []byte, key string, messageAttributes map[string]string) (string, error) {
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, data)
		if err != nil {
			return "", err
		}
	}
	msg := sarama.ProducerMessage{Value: sarama.ByteEncoder(binary), Topic: topic}
	if messageAttributes != nil {
		msg.Headers = MapToHeader(messageAttributes)
	}
	m := make(map[string]interface{})
	if len(key) > 0 {
		msg.Key = sarama.StringEncoder(key)
		m[Key] = key
	}
	pt, o, err := p.SyncProducer.SendMessage(&msg)
	m[Partition] = pt
	m[Offset] = o
	b, _ := json.Marshal(m)
	return string(b), err
}
