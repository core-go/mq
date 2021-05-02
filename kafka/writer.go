package kafka

import (
	"context"
	"crypto/tls"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"strings"
	"time"
)

type Writer struct {
	Writer *kafka.Writer
	Key    bool
}

func NewWriter(writer *kafka.Writer, generateKey bool) (*Writer, error) {
	return &Writer{Writer: writer, Key: generateKey}, nil
}

func NewWriterByConfig(c WriterConfig) (*Writer, error) {
	generateKey := true
	if c.Key != nil {
		generateKey = *c.Key
	}
	dialer := GetDialer(c.Client.Username, c.Client.Password, scram.SHA512, &kafka.Dialer{
		Timeout:   30 * time.Second,
		DualStack: true,
		TLS:       &tls.Config{},
	})
	writer := NewKafkaWriter(c.Topic, c.Brokers, dialer)
	return NewWriter(writer, generateKey)
}

func (p *Writer) Write(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	msg := kafka.Message{Value: data}
	if attributes != nil {
		msg.Headers = MapToHeader(attributes)
	}
	if p.Key {
		id := strings.Replace(uuid.New().String(), "-", "", -1)
		msg.Key = []byte(id)
		err := p.Writer.WriteMessages(ctx, msg)
		return id, err
	} else {
		err := p.Writer.WriteMessages(ctx, msg)
		return "", err
	}
}
