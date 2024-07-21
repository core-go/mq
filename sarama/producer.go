package kafka

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"time"
)

func MapToHeader(messageAttributes map[string]string) []sarama.RecordHeader {
	headers := make([]sarama.RecordHeader, 0)
	for k, v := range messageAttributes {
		h := sarama.RecordHeader{Key: []byte(k), Value: []byte(v)}
		headers = append(headers, h)
	}
	return headers
}

type Producer struct {
	SyncProducer sarama.SyncProducer
	Topic        string
}

func NewProducer(writer sarama.SyncProducer, topic string) (*Producer, error) {
	return &Producer{SyncProducer: writer, Topic: topic}, nil
}
func NewProducerByConfig(c ProducerConfig) (*Producer, error) {
	writer, err := newSyncProducer(c)
	if err != nil {
		return nil, err
	}
	return NewProducer(*writer, c.Topic)
}
func newSyncProducer(c ProducerConfig) (*sarama.SyncProducer, error) {
	if c.Client.Retry != nil && c.Client.Retry.Retry1 > 0 {
		durations := DurationsFromValue(*c.Client.Retry, "Retry", 9)
		return NewSyncProducerWithRetryArray(c, durations)
	} else {
		return NewSyncProducer(c)
	}
}
func NewSyncProducer(c ProducerConfig, retries ...time.Duration) (*sarama.SyncProducer, error) {
	l := len(retries)
	if l == 0 {
		return NewSyncProducerByConfig(c)
	} else {
		return NewSyncProducerWithRetryArray(c, retries)
	}
}
func NewSyncProducerWithRetryArray(c ProducerConfig, retries []time.Duration) (*sarama.SyncProducer, error) {
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
func NewSyncProducerByConfig(c ProducerConfig) (*sarama.SyncProducer, error) {
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
func (p *Producer) ProduceMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	return p.SyncProducer.SendMessage(msg)
}
func (p *Producer) Produce(ctx context.Context, data []byte, messageAttributes map[string]string) error {
	msg := sarama.ProducerMessage{Value: sarama.ByteEncoder(data), Topic: p.Topic}
	if messageAttributes != nil {
		msg.Headers = MapToHeader(messageAttributes)
	}
	_, _, err := p.SyncProducer.SendMessage(&msg)
	return err
}
func (p *Producer) ProduceValue(ctx context.Context, data []byte) error {
	msg := sarama.ProducerMessage{Value: sarama.ByteEncoder(data), Topic: p.Topic}
	_, _, err := p.SyncProducer.SendMessage(&msg)
	return err
}
func (p *Producer) ProduceWithKey(data []byte, key string, messageAttributes map[string]string) error {
	msg := sarama.ProducerMessage{Value: sarama.ByteEncoder(data), Topic: p.Topic}
	if messageAttributes != nil {
		msg.Headers = MapToHeader(messageAttributes)
	}
	if len(key) > 0 {
		msg.Key = sarama.StringEncoder(key)
	}
	_, _, err := p.SyncProducer.SendMessage(&msg)
	return err
}
