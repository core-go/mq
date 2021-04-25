package kafka

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/common-go/mq"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Reader struct {
	Reader       *kafka.Reader
	AckOnConsume bool
}

func NewReader(reader *kafka.Reader, ackOnConsume bool) (*Reader, error) {
	return &Reader{Reader: reader, AckOnConsume: ackOnConsume}, nil
}

func NewReaderByConfig(c ReaderConfig, ackOnConsume bool) (*Reader, error) {
	dialer := GetDialer(c.Client.Username, c.Client.Password, scram.SHA512, &kafka.Dialer{
		Timeout:   30 * time.Second,
		DualStack: true,
		TLS:       &tls.Config{},
	})
	reader := NewKafkaReader(c, dialer)
	return NewReader(reader, ackOnConsume)
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
				Raw:        msg,
			}
			if c.AckOnConsume {
				c.Reader.CommitMessages(ctx, msg)
			}
			handle(ctx, &message, nil)
		}
	}
}
