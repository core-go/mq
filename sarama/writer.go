package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

const (
	Key       = "key"
	Partition = "partition"
	Offset    = "offset"
)

type Writer struct {
	SyncProducer sarama.SyncProducer
	Topic        string
	Convert      func(context.Context, []byte) ([]byte, error)
	Generate     func() string
}

func NewWriter(writer sarama.SyncProducer, topic string, convert func(context.Context, []byte)([]byte, error), options ...func() string) (*Writer, error) {
	var generate func() string
	if len(options) > 0 {
		generate = options[0]
	}
	return &Writer{SyncProducer: writer, Topic: topic, Convert: convert, Generate: generate}, nil
}
func NewWriterByConfig(c WriterConfig, convert func(context.Context, []byte)([]byte, error), options ...func() string) (*Writer, error) {
	writer, err := newSyncProducer(c)
	if err != nil {
		return nil, err
	}
	return NewWriter(*writer, c.Topic, convert, options...)
}
func newSyncProducer(c WriterConfig) (*sarama.SyncProducer, error) {
	if c.Client.Retry != nil && c.Client.Retry.Retry1 > 0 {
		durations := DurationsFromValue(*c.Client.Retry, "Retry", 9)
		return NewSyncProducerWithRetryArray(c, durations)
	} else {
		return NewSyncProducer(c)
	}
}
func NewSyncProducer(c WriterConfig, retries ...time.Duration) (*sarama.SyncProducer, error) {
	l := len(retries)
	if l == 0 {
		return NewSyncProducerByConfig(c)
	} else {
		return NewSyncProducerWithRetryArray(c, retries)
	}
}
func NewSyncProducerWithRetryArray(c WriterConfig, retries []time.Duration) (*sarama.SyncProducer, error) {
	p, er1 := NewSyncProducerByConfig(c)
	if er1 == nil {
		return p, er1
	}
	i := 0
	err := Retry(retries, func() (err error) {
		i = i + 1
		p2, er2 := NewSyncProducerByConfig(c)
		p = p2
		if er2 == nil {
			log.Println(fmt.Sprintf("New SyncProducer after successfully %d retries", i))
		}
		return er2
	})
	if err != nil {
		log.Println(fmt.Sprintf("Failed to after successfully %d retries", i))
	}
	return p, err
}
func NewSyncProducerByConfig(c WriterConfig) (*sarama.SyncProducer, error) {
	conf := sarama.NewConfig()
	algorithm := sarama.SASLTypeSCRAMSHA256
	if c.Client.Algorithm != "" {
		algorithm = c.Client.Algorithm
	}
	config, er1 := GetConfig(c.Brokers, &algorithm, &c.Client, *conf)
	if er1 != nil {
		return nil, er1
	}
	config.Net.MaxOpenRequests = 1
	if c.MaxOpenRequests != nil {
		config.Net.MaxOpenRequests = *c.MaxOpenRequests
	}
	if c.Retry != nil {
		if c.Retry.Max != nil {
			config.Producer.Retry.Max = *c.Retry.Max
		}
		if c.Retry.Backoff > 0 {
			config.Producer.Retry.Backoff = time.Duration(c.Retry.Backoff) * time.Millisecond
		}
	}
	config.Producer.Idempotent = true
	config.Producer.Return.Successes = true
	if c.Idempotent != nil {
		config.Producer.Idempotent = *c.Idempotent
	}
	if c.ReturnSuccesses != nil {
		config.Producer.Return.Successes = *c.ReturnSuccesses
	}
	if c.RequiredAcks != nil {
		if *c.RequiredAcks == 0 {
			config.Producer.RequiredAcks = sarama.NoResponse
		} else if *c.RequiredAcks == 1 {
			config.Producer.RequiredAcks = sarama.WaitForLocal
		}
	}
	writer, er2 := sarama.NewSyncProducer(c.Brokers, config)
	if er2 != nil {
		return nil, er2
	}
	return &writer, nil
}

func (p *Writer) Write(ctx context.Context, data []byte, messageAttributes map[string]string) (string, error) {
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, data)
		if err != nil {
			return "", err
		}
	}
	msg := sarama.ProducerMessage{Value: sarama.ByteEncoder(binary), Topic: p.Topic}
	if messageAttributes != nil {
		msg.Headers = MapToHeader(messageAttributes)
	}
	if p.Generate != nil {
		id := p.Generate()
		msg.Key = sarama.StringEncoder(id)
		p, o, err := p.SyncProducer.SendMessage(&msg)
		m := make(map[string]interface{})
		m[Key] = id
		m[Partition] = p
		m[Offset] = o
		b, _ := json.Marshal(m)
		return string(b), err
	} else {
		p, o, err := p.SyncProducer.SendMessage(&msg)
		m := make(map[string]interface{})
		m[Partition] = p
		m[Offset] = o
		b, _ := json.Marshal(m)
		return string(b), err
	}
}
