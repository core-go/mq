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
	}
)

func NewProducerByConfig(c ProducerConfig) (*Producer, error) {
	p, err := NewKafkaProducerByConfig(c)
	if err != nil {
		fmt.Printf("Failed to create Producer: %s\n", err)
		return nil, err
	}

	return &Producer{
		Producer: p,
		Topic:    c.Topic,
	}, nil
}
func NewProducer(producer *kafka.Producer, topic string) *Producer {
	return &Producer{ Producer: producer, Topic: topic}
}
func NewProducerByConfigAndRetries(c ProducerConfig, retries ...time.Duration) (*Producer, error) {
	if len(retries) == 0 {
		return NewProducerByConfig(c)
	} else {
		return NewProducerWithRetryArray(c, retries)
	}
}

func NewProducerWithRetryArray(c ProducerConfig, retries []time.Duration) (*Producer, error) {
	p, err := NewProducerByConfig(c)
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
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.Topic, Partition: kafka.PartitionAny},
		Value:          data}
	if messageAttributes != nil {
		msg.Headers = MapToHeader(messageAttributes)
	}

	return Produce(p.Producer, &msg)
}
