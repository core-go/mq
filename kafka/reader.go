package kafka

import (
	"context"
	"crypto/tls"
	"github.com/core-go/mq"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"time"
)

type Reader struct {
	Reader       *kafka.Reader
	AckOnConsume bool
	Convert      func(context.Context, []byte) ([]byte, error)
}

func NewReader(reader *kafka.Reader, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) (*Reader, error) {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Reader{Reader: reader, AckOnConsume: ackOnConsume, Convert: convert}, nil
}

func NewReaderByConfig(c ReaderConfig, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) (*Reader, error) {
	dialer := GetDialer(c.Client.Username, c.Client.Password, scram.SHA512, &kafka.Dialer{
		Timeout:   30 * time.Second,
		DualStack: true,
		TLS:       &tls.Config{},
	})
	reader := NewKafkaReader(c, dialer)
	return NewReader(reader, ackOnConsume, options...)
}

func (c *Reader) Read(ctx context.Context, handle func(context.Context, *mq.Message, error) error) {
	for {
		msg, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			handle(ctx, nil, err)
		} else {
			attributes := HeaderToMap(msg.Headers)
			message := mq.Message{
				Id:         string(msg.Key),
				Data:       msg.Value,
				Attributes: attributes,
				Timestamp:  &msg.Time,
				Raw:        msg,
			}
			if c.AckOnConsume {
				c.Reader.CommitMessages(ctx, msg)
			}
			if c.Convert == nil {
				handle(ctx, &message, nil)
			} else {
				data, err := c.Convert(ctx, msg.Value)
				if err == nil {
					message.Data = data
				}
				handle(ctx, &message, err)
			}

		}
	}
}
