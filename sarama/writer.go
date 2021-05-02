package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"log"
	"strings"
	"time"
)

const (
	Key       = "key"
	Partition = "partition"
	Offset    = "offset"
)

type Writer struct {
	SyncProducer sarama.SyncProducer
	Key          bool
	Topic        string
}

func NewWriter(writer sarama.SyncProducer, generateKey bool, topic string) (*Writer, error) {
	return &Writer{SyncProducer: writer, Key: generateKey, Topic: topic}, nil
}
func NewWriterByConfig(c WriterConfig) (*Writer, error) {
	generateKey := true
	if c.Key != nil {
		generateKey = *c.Key
	}
	writer, err := newSyncProducer(c)
	if err != nil {
		return nil, err
	}
	return NewWriter(*writer, generateKey, c.Topic)
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
	msg := sarama.ProducerMessage{Value: sarama.ByteEncoder(data), Topic: p.Topic}
	if messageAttributes != nil {
		msg.Headers = MapToHeader(messageAttributes)
	}
	if p.Key {
		id := strings.Replace(uuid.New().String(), "-", "", -1)
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
