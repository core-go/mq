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
		Timeout  int
		Convert  func(context.Context, []byte) ([]byte, error)
		Generate func() string
		Error    func(*kafka.Message, error) error
	}
)
func NewProducerByConfigMap(c kafka.ConfigMap, topic string, timeout int, convert func(context.Context, []byte) ([]byte, error), options ...func() string) (*Producer, error) {
	p, err := kafka.NewProducer(&c)
	if err != nil {
		fmt.Printf("Failed to create Producer: %s\n", err)
		return nil, err
	}
	var generate func() string
	if len(options) > 0 {
		generate = options[0]
	}
	if timeout <= 0 {
		timeout = 100
	}
	pd := &Producer{
		Producer: p,
		Topic:    topic,
		Timeout:  timeout,
		Convert:  convert,
		Generate: generate,
	}
	return pd, nil
}
func NewProducerByConfig(c ProducerConfig, convert func(context.Context, []byte) ([]byte, error), options ...func() string) (*Producer, error) {
	p, err := NewKafkaProducerByConfig(c)
	if err != nil {
		fmt.Printf("Failed to create Producer: %s\n", err)
		return nil, err
	}
	var generate func() string
	if len(options) > 0 {
		generate = options[0]
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 100
	}
	pd := &Producer{
		Producer: p,
		Topic:    c.Topic,
		Timeout:  timeout,
		Convert:  convert,
		Generate: generate,
	}
	return pd, nil
}
func NewProducer(producer *kafka.Producer, topic string, timeout int, convert func(context.Context, []byte) ([]byte, error), options ...func() string) *Producer {
	var generate func() string
	if len(options) > 0 {
		generate = options[0]
	}
	if timeout <= 0 {
		timeout = 100
	}
	return &Producer{Producer: producer, Topic: topic, Timeout: timeout, Convert: convert, Generate: generate}
}
func NewProducerByConfigAndRetries(c ProducerConfig, convert func(context.Context, []byte) ([]byte, error), retries ...time.Duration) (*Producer, error) {
	if len(retries) == 0 {
		return NewProducerByConfig(c, convert)
	} else {
		return NewProducerWithRetryArray(c, retries, convert)
	}
}

func NewProducerWithRetryArray(c ProducerConfig, retries []time.Duration, convert func(context.Context, []byte) ([]byte, error), options ...func() string) (*Producer, error) {
	p, err := NewProducerByConfig(c, convert, options...)
	if err == nil {
		return p, nil
	}

	i := 0
	err = Retry(retries, func() (err error) {
		i = i + 1
		p2, er2 := NewProducerByConfig(c, convert, options...)
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
	if p.Generate != nil {
		id := p.Generate()
		msg.Key = []byte(id)
	}
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)
	err = p.Producer.Produce(&msg, deliveryChan)
	if err != nil {
		return msg.String(), err
	}
	p.Producer.Flush(p.Timeout)
	e := <-deliveryChan
	switch m := e.(type) {
	case *kafka.Message:
		if m.TopicPartition.Error != nil {
			if p.Error != nil {
				err = p.Error(m, err)
			}
			return msg.String(), err
		}
		return msg.String(), m.TopicPartition.Error
	case kafka.Error:
		return "", m
	}
	return msg.String(), nil
}
func (p *Producer) ProduceWithKey(ctx context.Context, data []byte, key []byte, messageAttributes map[string]string) (string, error) {
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
	if key != nil {
		msg.Key = key
	}
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)
	err = p.Producer.Produce(&msg, deliveryChan)
	if err != nil {
		return msg.String(), err
	}
	p.Producer.Flush(p.Timeout)
	e := <-deliveryChan
	switch m := e.(type) {
	case *kafka.Message:
		if m.TopicPartition.Error != nil {
			if p.Error != nil {
				err = p.Error(m, err)
			}
			return msg.String(), err
		}
		return msg.String(), m.TopicPartition.Error
	case kafka.Error:
		return "", m
	}
	return msg.String(), nil
}
