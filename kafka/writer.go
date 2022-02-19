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
	Convert func(context.Context, []byte)([]byte, error)
	Generate func()string
}

func NewWriter(writer *kafka.Writer, convert func(context.Context, []byte)([]byte, error), options...func()string) (*Writer, error) {
	var generate func()string
	if len(options) > 0 {
		generate = options[0]
	}
	return &Writer{Writer: writer, Convert: convert, Generate: generate}, nil
}

func NewWriterByConfig(c WriterConfig, convert func(context.Context, []byte)([]byte, error), options...func()string) (*Writer, error) {
	dialer := GetDialer(c.Client.Username, c.Client.Password, scram.SHA512, &kafka.Dialer{
		Timeout:   30 * time.Second,
		DualStack: true,
		TLS:       &tls.Config{},
	})
	writer := NewKafkaWriter(c.Topic, c.Brokers, dialer)
	return NewWriter(writer, convert, options...)
}
func (p *Writer) Put(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Write(ctx, data, attributes)
}
func (p *Writer) Send(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Write(ctx, data, attributes)
}
func (p *Writer) Produce(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Write(ctx, data, attributes)
}
func (p *Writer) Publish(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Write(ctx, data, attributes)
}
func (p *Writer) Write(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, data)
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

		err = p.Writer.WriteMessages(ctx, msg)
		return id, err
	} else {
		err = p.Writer.WriteMessages(ctx, msg)
		return "", err
	}
}
func (p *Writer) WriteWithKey(ctx context.Context, data []byte, key []byte, attributes map[string]string) (string, error) {
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, data)
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
