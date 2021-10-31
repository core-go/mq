package kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

type (
	Producer struct {
		Producer *kafka.Producer
		Topic    string
		Convert  func(context.Context, []byte) ([]byte, error)
	}
)

func NewProducerByConfig(c ProducerConfig, options ...func(context.Context, []byte) ([]byte, error)) (*Producer, error) {
	p, err := NewKafkaProducerByConfig(c)
	if err != nil {
		fmt.Printf("Failed to create Producer: %s\n", err)
		return nil, err
	}
	var convert func(context.Context, []byte) ([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Producer{
		Producer: p,
		Topic:    c.Topic,
		Convert:  convert,
	}, nil
}
func NewProducer(producer *kafka.Producer, topic string, options ...func(context.Context, []byte)([]byte, error)) *Producer {
	var convert func(context.Context, []byte) ([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Producer{Producer: producer, Topic: topic, Convert: convert}
}
func NewProducerByConfigAndRetries(c ProducerConfig, convert func(context.Context, []byte)([]byte, error), retries ...time.Duration) (*Producer, error) {
	if len(retries) == 0 {
		return NewProducerByConfig(c, convert)
	} else {
		return NewProducerWithRetryArray(c, retries, convert)
	}
}

func NewProducerWithRetryArray(c ProducerConfig, retries []time.Duration, options...func(context.Context, []byte)([]byte, error)) (*Producer, error) {
	p, err := NewProducerByConfig(c, options...)
	if err == nil {
		return p, nil
	}

	i := 0
	err = Retry(retries, func() (err error) {
		i = i + 1
		p2, er2 := NewProducerByConfig(c)
		p = p2
		if er2 == nil {
			log.Println(fmt.Sprintf("create new Producer successfully after %d retries", i))
		}
		return er2
	})
	if err != nil {
		log.Println(fmt.Sprintf("fail in creating new Producer after %d retries", i))
	}
	return p, err
}

func (p *Producer) Produce(ctx context.Context, data []byte, messageAttributes map[string]string) (string, error) {
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, data)
		if err != nil {
			return "", err
		}
	}
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.Topic, Partition: kafka.PartitionAny},
		Value:          binary}
	if messageAttributes != nil {
		msg.Headers = MapToHeader(messageAttributes)
	}

	return Produce(p.Producer, &msg)
}
