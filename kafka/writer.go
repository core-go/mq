package kafka

import (
	"context"
	"crypto/tls"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"time"
)

type Writer struct {
	Writer *kafka.Writer
	Generate func()string
}

func NewWriter(writer *kafka.Writer, options...func()string) (*Writer, error) {
	var generate func()string
	if len(options) > 0 {
		generate = options[0]
	}
	return &Writer{Writer: writer, Generate: generate}, nil
}

func NewWriterByConfig(c WriterConfig, options...func()string) (*Writer, error) {
	dialer := GetDialer(c.Client.Username, c.Client.Password, scram.SHA512, &kafka.Dialer{
		Timeout:   30 * time.Second,
		DualStack: true,
		TLS:       &tls.Config{},
	})
	writer := NewKafkaWriter(c.Topic, c.Brokers, dialer)
	return NewWriter(writer, options...)
}

func (p *Writer) Write(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	msg := kafka.Message{Value: data}
	if attributes != nil {
		msg.Headers = MapToHeader(attributes)
	}
	if p.Generate != nil {
		id := p.Generate()
		msg.Key = []byte(id)
		err := p.Writer.WriteMessages(ctx, msg)
		return id, err
	} else {
		err := p.Writer.WriteMessages(ctx, msg)
		return "", err
	}
}
