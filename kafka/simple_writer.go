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
	Convert func(context.Context, []byte)([]byte, error)
	Generate func()string
}

func NewSimpleWriter(writer *kafka.Writer, convert func(context.Context, []byte)([]byte, error), options...func()string) (*SimpleWriter, error) {
	var generate func()string
	if len(options) > 0 {
		generate = options[0]
	}
	return &SimpleWriter{Writer: writer, Convert: convert, Generate: generate}, nil
}

func NewSimpleWriterByConfig(c WriterConfig, convert func(context.Context, []byte)([]byte, error), options...func()string) (*SimpleWriter, error) {
	dialer := GetDialer(c.Client.Username, c.Client.Password, scram.SHA512, &kafka.Dialer{
		Timeout:   30 * time.Second,
		DualStack: true,
		TLS:       &tls.Config{},
	})
	writer := NewKafkaWriter(c.Topic, c.Brokers, dialer)
	return NewSimpleWriter(writer, convert, options...)
}

func (p *SimpleWriter) Write(ctx context.Context, topic string, data []byte, attributes map[string]string) (string, error) {
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, binary)
		if err != nil {
			return "", err
		}
	}
	msg := kafka.Message{Value: binary}
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
func (p *SimpleWriter) WriteWithKey(ctx context.Context, topic string, data []byte, key []byte, attributes map[string]string) (string, error) {
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, binary)
		if err != nil {
			return "", err
		}
	}
	msg := kafka.Message{Value: binary}
	if attributes != nil {
		msg.Headers = MapToHeader(attributes)
	}
	if key != nil {
		msg.Key = key
	}
	err = p.Writer.WriteMessages(ctx, msg)
	return "", err
}
