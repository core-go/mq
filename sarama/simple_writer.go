package kafka

import (
	"context"
	"encoding/json"
	"github.com/Shopify/sarama"
)

type SimpleWriter struct {
	SyncProducer sarama.SyncProducer
	Generate     func() string
}

func NewSimpleWriter(writer sarama.SyncProducer, options ...func() string) (*SimpleWriter, error) {
	var generate func()string
	if len(options) > 0 {
		generate = options[0]
	}
	return &SimpleWriter{SyncProducer: writer, Generate: generate}, nil
}
func NewSimpleWriterByConfig(c WriterConfig, options ...func() string) (*SimpleWriter, error) {
	writer, err := newSyncProducer(c)
	if err != nil {
		return nil, err
	}
	return NewSimpleWriter(*writer, options...)
}
func (p *SimpleWriter) Write(ctx context.Context, topic string, data []byte, messageAttributes map[string]string) (string, error) {
	msg := sarama.ProducerMessage{Value: sarama.ByteEncoder(data), Topic: topic}
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
