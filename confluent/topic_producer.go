package kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strings"
)

type (
	TopicProducer struct {
		Producer *kafka.Producer
		Timeout  int
		Convert  func(context.Context, []byte) ([]byte, error)
		Generate func() string
		Error    func(*kafka.Message, error) error
	}
)

func NewTopicProducerByConfigMap(c kafka.ConfigMap, timeout int, convert func(context.Context, []byte) ([]byte, error), options ...func() string) (*TopicProducer, error) {
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
	pd := &TopicProducer{
		Producer: p,
		Timeout:  timeout,
		Convert:  convert,
		Generate: generate,
	}
	return pd, nil
}
func NewTopicProducerByConfig(c ProducerConfig, timeout int, convert func(context.Context, []byte) ([]byte, error), options ...func() string) (*TopicProducer, error) {
	p, err := NewKafkaProducerByConfig(c)
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
	pd := &TopicProducer{
		Producer: p,
		Timeout:  timeout,
		Convert:  convert,
		Generate: generate,
	}
	return pd, nil
}
func NewTopicProducer(producer *kafka.Producer, timeout int, convert func(context.Context, []byte) ([]byte, error), options ...func() string) *TopicProducer {
	var generate func() string
	if len(options) > 0 {
		generate = options[0]
	}
	if timeout <= 0 {
		timeout = 100
	}
	return &TopicProducer{Producer: producer, Timeout: timeout, Convert: convert, Generate: generate}
}
func NewKafkaProducerByConfig(c ProducerConfig) (*kafka.Producer, error) {
	conf := kafka.ConfigMap{
		"bootstrap.servers": strings.Join(c.Brokers, ","),
	}

	if c.RequiredAcks != nil {
		conf["acks"] = *c.RequiredAcks
	}

	if c.Client.TLSEnable == nil || *c.Client.TLSEnable == false {
		conf["security.protocol"] = ProtocolSSL
		conf["sasl.username"] = *c.Client.Username
		conf["sasl.password"] = *c.Client.Password
	} else {
		panic("not supported yet")
	}

	if c.Client.Algorithm == "" {
		conf["sasl.mechanism"] = SASLTypeSCRAMSHA256
	} else {
		conf["sasl.mechanism"] = c.Client.Algorithm
	}

	if c.Retry != nil && (c.Retry.Max != nil && *c.Retry.Max > 0) {
		conf["retries"] = *c.Retry.Max
		if c.Retry.Backoff > 0 {
			conf["retry.backoff.ms"] = c.Retry.Backoff
		}
	}

	return kafka.NewProducer(&conf)
}
func (p *TopicProducer) Produce(ctx context.Context, topic string, data []byte, messageAttributes map[string]string) error {
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, data)
		if err != nil {
			return err
		}
	}
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
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
		return err
	}
	p.Producer.Flush(p.Timeout)
	e := <-deliveryChan
	switch m := e.(type) {
	case *kafka.Message:
		if m.TopicPartition.Error != nil {
			if p.Error != nil {
				err = p.Error(m, err)
			}
			return err
		}
		return m.TopicPartition.Error
	case kafka.Error:
		return m
	}
	return nil
}
func (p *TopicProducer) ProduceWithKey(ctx context.Context, topic string, key []byte, data []byte, messageAttributes map[string]string) error {
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, data)
		if err != nil {
			return err
		}
	}
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
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
		return err
	}
	p.Producer.Flush(p.Timeout)
	e := <-deliveryChan
	switch m := e.(type) {
	case *kafka.Message:
		if m.TopicPartition.Error != nil {
			if p.Error != nil {
				err = p.Error(m, err)
			}
			return err
		}
		return m.TopicPartition.Error
	case kafka.Error:
		return m
	}
	return nil
}
func MapToHeader(messageAttributes map[string]string) []kafka.Header {
	headers := make([]kafka.Header, 0)
	for k, v := range messageAttributes {
		h := kafka.Header{Key: k, Value: []byte(v)}
		headers = append(headers, h)
	}
	return headers
}
