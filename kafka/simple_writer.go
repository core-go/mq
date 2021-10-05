package kafka

import (
	"context"
	"crypto/tls"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"time"
)

type SimpleWriter struct {
	Writer   *kafka.Writer
	Generate func()string
}

func NewSimpleWriter(writer *kafka.Writer, options...func()string) (*SimpleWriter, error) {
	var generate func()string
	if len(options) > 0 {
		generate = options[0]
	}
	return &SimpleWriter{Writer: writer, Generate: generate}, nil
}

func NewSimpleWriterByConfig(c WriterConfig, options...func()string) (*SimpleWriter, error) {
	dialer := GetDialer(c.Client.Username, c.Client.Password, scram.SHA512, &kafka.Dialer{
		Timeout:   30 * time.Second,
		DualStack: true,
		TLS:       &tls.Config{},
	})
	writer := NewKafkaWriter(c.Topic, c.Brokers, dialer)
	return NewSimpleWriter(writer, options...)
}

func (p *SimpleWriter) Write(ctx context.Context, topic string, data []byte, attributes map[string]string) (string, error) {
	msg := kafka.Message{Value: data}
	if attributes != nil {
		msg.Headers = MapToHeader(attributes)
	}
	if p.Generate != nil {
		id := p.Generate()
		msg.Key = []byte(id)
		p.Writer.Topic = topic
		err := p.Writer.WriteMessages(ctx, msg)
		return id, err
	} else {
		p.Writer.Topic = topic
		err := p.Writer.WriteMessages(ctx, msg)
		return "", err
	}
}
