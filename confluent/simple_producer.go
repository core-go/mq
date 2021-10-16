package kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strings"
)

type (
	SimpleProducer struct {
		Producer *kafka.Producer
	}
)

func NewSimpleProducerByConfig(c ProducerConfig) (*SimpleProducer, error) {
	p, err := NewKafkaProducerByConfig(c)
	if err != nil {
		fmt.Printf("Failed to create Producer: %s\n", err)
		return nil, err
	}

	return &SimpleProducer{
		Producer: p,
	}, nil
}
func NewSimpleProducer(producer *kafka.Producer) *SimpleProducer {
	return &SimpleProducer{Producer: producer}
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

func (p *SimpleProducer) Produce(ctx context.Context, topic string, data []byte, messageAttributes map[string]string) (string, error) {
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          data}
	if messageAttributes != nil {
		msg.Headers = MapToHeader(messageAttributes)
	}
	return Produce(p.Producer, &msg)
}

func Produce(producer *kafka.Producer, msg *kafka.Message) (string, error) {
	deliveryChan := make(chan kafka.Event, 10000)
	err := producer.Produce(msg, deliveryChan)
	if err != nil {
		fmt.Printf("Failed to produce msg: %s\n", err)
		return "", err
	}
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Successfully produced record to Topic %s partition [%d] @ offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	return msg.String(), nil
}

func MapToHeader(messageAttributes map[string]string) []kafka.Header {
	headers := make([]kafka.Header, 0)
	for k, v := range messageAttributes {
		h := kafka.Header{Key: k, Value: []byte(v)}
		headers = append(headers, h)
	}
	return headers
}
