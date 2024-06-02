package kafka

import (
	"context"
	"crypto/tls"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"time"
)

type Reader struct {
	Reader       *kafka.Reader
	LogError     func(ctx context.Context, msg string)
	AckOnConsume bool
	Key          string
}

func NewReader(reader *kafka.Reader, logError func(ctx context.Context, msg string), ackOnConsume bool, key string) (*Reader, error) {
	return &Reader{Reader: reader, LogError: logError, AckOnConsume: ackOnConsume, Key: key}, nil
}

func NewReaderByConfig(c ReaderConfig, logError func(ctx context.Context, msg string), ackOnConsume bool) (*Reader, error) {
	dialer := GetDialer(c.Client.Username, c.Client.Password, scram.SHA512, &kafka.Dialer{
		Timeout:   30 * time.Second,
		DualStack: true,
		TLS:       &tls.Config{},
	})
	reader := NewKafkaReader(c, dialer)
	return NewReader(reader, logError, ackOnConsume, c.Key)
}

func (c *Reader) Read(ctx context.Context, handle func(context.Context, []byte, map[string]string)) {
	for {
		msg, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			c.LogError(ctx, "Error when read: "+err.Error())
		} else {
			attributes := HeaderToMap(msg.Headers)
			if len(c.Key) > 0 && msg.Key != nil {
				ctx = context.WithValue(ctx, c.Key, string(msg.Key))
			}
			if c.AckOnConsume {
				c.Reader.CommitMessages(ctx, msg)
			}
			handle(ctx, msg.Value, attributes)
		}
	}
}
func (c *Reader) ReadValue(ctx context.Context, handle func(context.Context, []byte)) {
	for {
		msg, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			c.LogError(ctx, "Error when read: "+err.Error())
		} else {
			if len(c.Key) > 0 && msg.Key != nil {
				ctx = context.WithValue(ctx, c.Key, string(msg.Key))
			}
			if c.AckOnConsume {
				c.Reader.CommitMessages(ctx, msg)
			}
			handle(ctx, msg.Value)
		}
	}
}
func (c *Reader) ReadMessage(ctx context.Context, handle func(context.Context, kafka.Message)) {
	for {
		msg, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			c.LogError(ctx, "Error when read: "+err.Error())
		} else {
			if len(c.Key) > 0 && msg.Key != nil {
				ctx = context.WithValue(ctx, c.Key, string(msg.Key))
			}
			if c.AckOnConsume {
				c.Reader.CommitMessages(ctx, msg)
			}
			handle(ctx, msg)
		}
	}
}
