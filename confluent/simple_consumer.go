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
		LogError func(context.Context, string)
		LogInfo  func(context.Context, string)
	}
)
func NewSimpleConsumerByConfigMap(conf kafka.ConfigMap, topics []string, logs ...func(context.Context, string)) (*SimpleConsumer, error) {
	consumer, err := kafka.NewConsumer(&conf)
	if err != nil {
		fmt.Printf("Failed to create Consumer: %s\n", err)
		return nil, err
	}
	cs := &SimpleConsumer{
		Consumer: consumer,
		Topics:   topics,
	}
	if len(logs) >= 1 {
		cs.LogError = logs[0]
	}
	if len(logs) >= 2 {
		cs.LogInfo = logs[1]
	}
	return cs, nil
}
func NewSimpleConsumerByConfig(c ConsumerConfig, logs ...func(context.Context, string)) (*SimpleConsumer, error) {
	consumer, err := NewKafkaConsumerByConfig(c)
	if err != nil {
		fmt.Printf("Failed to create Consumer: %s\n", err)
		return nil, err
	}
	cs := &SimpleConsumer{
		Consumer: consumer,
		Topics:   []string{c.Topic},
	}
	if len(logs) >= 1 {
		cs.LogError = logs[0]
	}
	if len(logs) >= 2 {
		cs.LogInfo = logs[1]
	}
	return cs, nil
}
func NewSimpleConsumer(consumer *kafka.Consumer, topics []string, logs ...func(context.Context, string)) *SimpleConsumer {
	c := &SimpleConsumer{Consumer: consumer, Topics: topics}
	if len(logs) >= 1 {
		c.LogError = logs[0]
	}
	if len(logs) >= 2 {
		c.LogInfo = logs[1]
	}
	return c
}
func NewSimpleConsumerWithConvert(consumer *kafka.Consumer, topics []string, options ...func(context.Context, []byte) ([]byte, error)) *SimpleConsumer {
	var convert func(context.Context, []byte) ([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &SimpleConsumer{Consumer: consumer, Topics: topics, Convert: convert}
}
func (c *SimpleConsumer) Consume(ctx context.Context, handle func(context.Context, []byte, map[string]string, error) error) {
	defer c.Consumer.Close()

	err := c.Consumer.SubscribeTopics(c.Topics, nil)
	if err != nil {
		if c.LogError != nil {
			c.LogError(ctx, fmt.Sprintf("Subscribe Topic err: %v", err))
		}
		return
	}
	run := true
	for run == true {
		ev := c.Consumer.Poll(0)
		switch e := ev.(type) {
		case *kafka.Message:
			if c.LogInfo != nil {
				c.LogInfo(ctx, fmt.Sprintf("Message on %s: %s", e.TopicPartition, string(e.Value)))
			}
			h := HeaderToMap(e.Headers)
			if c.Convert == nil {
				handle(ctx, e.Value, h, nil)
			} else {
				data, err := c.Convert(ctx, e.Value)
				handle(ctx, data, h, err)
			}
		case kafka.PartitionEOF:
			if c.LogInfo != nil {
				c.LogInfo(ctx, fmt.Sprintf("Reached %v", e))

			}
		case kafka.Error:
			if c.LogError != nil {
				c.LogError(ctx, fmt.Sprintf("Error: %v", e))
			}
			handle(ctx, nil, nil, e)
			run = false
		default:
		}
	}
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
