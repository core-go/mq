package kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strings"
)

type (
	SimpleConsumer struct {
		Consumer *kafka.Consumer
		Topics   []string
		Convert  func(context.Context, []byte) ([]byte, error)
	}
)

func NewSimpleConsumerByConfig(c ConsumerConfig, options ...func(context.Context, []byte) ([]byte, error)) (*SimpleConsumer, error) {
	consumer, err := NewKafkaConsumerByConfig(c)
	if err != nil {
		fmt.Printf("Failed to create Consumer: %s\n", err)
		return nil, err
	}
	var convert func(context.Context, []byte) ([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &SimpleConsumer{
		Consumer: consumer,
		Topics:   []string{c.Topic},
		Convert:  convert,
	}, nil
}
func NewSimpleConsumer(consumer *kafka.Consumer, topics []string, options ...func(context.Context, []byte) ([]byte, error)) *SimpleConsumer {
	var convert func(context.Context, []byte) ([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &SimpleConsumer{Consumer: consumer, Topics: topics, Convert: convert}
}
func (c *SimpleConsumer) Consume(ctx context.Context, handle func(context.Context, []byte, map[string]string, error) error) {
	Consume(ctx, c.Consumer, c.Topics, handle, c.Convert)
}

func NewKafkaConsumerByConfig(c ConsumerConfig) (*kafka.Consumer, error) {
	conf := kafka.ConfigMap{
		"bootstrap.servers": strings.Join(c.Brokers, ","),
		"group.id":          c.GroupID,
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

	if c.InitialOffsets == nil {
		conf["auto.offset.reset"] = kafka.OffsetBeginning
	} else {
		conf["auto.offset.reset"] = kafka.Offset(*c.InitialOffsets)
	}

	if c.AckOnConsume {
		conf["enable.auto.commit"] = true
	} else {
		conf["enable.auto.commit"] = false
	}

	return kafka.NewConsumer(&conf)
}
