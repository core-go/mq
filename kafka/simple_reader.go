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
}

func NewSimpleReader(reader *kafka.Reader, ackOnConsume bool) (*SimpleReader, error) {
	return &SimpleReader{SimpleReader: reader, AckOnConsume: ackOnConsume}, nil
}

func NewSimpleReaderByConfig(c ReaderConfig, ackOnConsume bool) (*SimpleReader, error) {
	dialer := GetDialer(c.Client.Username, c.Client.Password, scram.SHA512, &kafka.Dialer{
		Timeout:   30 * time.Second,
		DualStack: true,
		TLS:       &tls.Config{},
	})
	reader := NewKafkaReader(c, dialer)
	return NewSimpleReader(reader, ackOnConsume)
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
			handle(ctx, msg.Value, attributes, nil)
		}
	}
}
