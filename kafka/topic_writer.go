package kafka

import (
	"context"
	"crypto/tls"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"time"
)

type TopicWriter struct {
	Writer   *kafka.Writer
	Generate func() string
}

func NewTopicWriter(writer *kafka.Writer, options ...func() string) (*TopicWriter, error) {
	var generate func() string
	if len(options) > 0 {
		generate = options[0]
	}
	return &TopicWriter{Writer: writer, Generate: generate}, nil
}

func NewTopicWriterByConfig(c WriterConfig, options ...func() string) (*TopicWriter, error) {
	if c.Client.Timeout <= 0 {
		c.Client.Timeout = 30
	}
	dialer := GetDialer(c.Client.Username, c.Client.Password, scram.SHA512, &kafka.Dialer{
		Timeout:   time.Duration(c.Client.Timeout) * time.Second,
		DualStack: true,
		TLS:       &tls.Config{},
	})
	writer := NewKafkaWriter(c.Topic, c.Brokers, dialer)
	return NewTopicWriter(writer, options...)
}
func (p *TopicWriter) Write(ctx context.Context, topic string, data []byte, attributes map[string]string) error {
	msg := kafka.Message{Value: data}
	if attributes != nil {
		msg.Headers = MapToHeader(attributes)
	}
	if p.Generate != nil {
		id := p.Generate()
		msg.Key = []byte(id)
		p.Writer.Topic = topic
		err := p.Writer.WriteMessages(ctx, msg)
		return err
	} else {
		p.Writer.Topic = topic
		err := p.Writer.WriteMessages(ctx, msg)
		return err
	}
}
func (p *TopicWriter) WriteValue(ctx context.Context, topic string, data []byte) error {
	return p.Write(ctx, topic, data, nil)
}
func (p *TopicWriter) WriteWithKey(ctx context.Context, topic string, data []byte, key []byte, attributes map[string]string) (string, error) {
	var binary = data
	var err error
	msg := kafka.Message{Value: binary}
	if attributes != nil {
		msg.Headers = MapToHeader(attributes)
	}
	if key != nil {
		msg.Key = key
	}
	p.Writer.Topic = topic
	err = p.Writer.WriteMessages(ctx, msg)
	return "", err
}
