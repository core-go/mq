package kafka

import (
	"context"
	"crypto/tls"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"time"
)

type SimpleReader struct {
	SimpleReader       *kafka.Reader
	AckOnConsume bool
	Convert func(context.Context, []byte)([]byte, error)
}

func NewSimpleReader(reader *kafka.Reader, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) (*SimpleReader, error) {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &SimpleReader{SimpleReader: reader, AckOnConsume: ackOnConsume, Convert: convert}, nil
}

func NewSimpleReaderByConfig(c ReaderConfig, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) (*SimpleReader, error) {
	dialer := GetDialer(c.Client.Username, c.Client.Password, scram.SHA512, &kafka.Dialer{
		Timeout:   30 * time.Second,
		DualStack: true,
		TLS:       &tls.Config{},
	})
	reader := NewKafkaReader(c, dialer)
	return NewSimpleReader(reader, ackOnConsume, options...)
}

func (c *SimpleReader) Read(ctx context.Context, handle func(context.Context, []byte, map[string]string, error) error) {
	for {
		msg, err := c.SimpleReader.FetchMessage(ctx)
		if err != nil {
			handle(ctx, nil, nil, err)
		} else {
			attributes := HeaderToMap(msg.Headers)
			if c.AckOnConsume {
				c.SimpleReader.CommitMessages(ctx, msg)
			}
			if c.Convert == nil {
				handle(ctx, msg.Value, attributes, nil)
			} else {
				data, err := c.Convert(ctx, msg.Value)
				handle(ctx, data, attributes, err)
			}
		}
	}
}
