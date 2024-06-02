package kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

type (
	Consumer struct {
		Consumer *kafka.Consumer
		Topics   []string
		LogError func(context.Context, string)
		LogInfo  func(context.Context, string)
	}
)

func NewConsumerByConfig(c ConsumerConfig, logs ...func(context.Context, string)) (*Consumer, error) {
	if c.Client.Retry != nil && c.Client.Retry.Retry1 > 0 {
		durations := DurationsFromValue(*c.Client.Retry, "Retry", 9)
		return NewConsumerByConfigAndRetryArray(c, durations)
	} else {
		consumer, err := NewKafkaConsumerByConfig(c)
		if err != nil {
			fmt.Printf("Failed to create Consumer: %s\n", err)
			return nil, err
		}
		cs := &Consumer{
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
}
func NewConsumerByConfigMap(conf kafka.ConfigMap, topics []string, logs ...func(context.Context, string)) (*Consumer, error) {
	consumer, err := kafka.NewConsumer(&conf)
	if err != nil {
		fmt.Printf("Failed to create Consumer: %s\n", err)
		return nil, err
	}
	cs := &Consumer{
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
func NewConsumer(consumer *kafka.Consumer, topics []string, logs ...func(context.Context, string)) *Consumer {
	c := &Consumer{Consumer: consumer, Topics: topics}
	if len(logs) >= 1 {
		c.LogError = logs[0]
	}
	if len(logs) >= 2 {
		c.LogInfo = logs[1]
	}
	return c
}
func NewConsumerByConfigAndRetries(c ConsumerConfig, retries ...time.Duration) (*Consumer, error) {
	if len(retries) == 0 {
		cs, err := NewConsumerByConfig(c)
		if err != nil {
			return cs, err
		}
		return cs, nil
	} else {
		return NewConsumerByConfigAndRetryArray(c, retries)
	}
}

func NewConsumerByConfigAndRetryArray(c ConsumerConfig, retries []time.Duration, options ...func(context.Context, []byte) ([]byte, error)) (*Consumer, error) {
	consumer, er1 := NewKafkaConsumerByConfig(c)
	if er1 == nil {
		return nil, er1
	}

	i := 0
	err := Retry(retries, func() (err error) {
		i = i + 1
		r2, er2 := NewKafkaConsumerByConfig(c)
		consumer = r2
		if er2 == nil {
			log.Println(fmt.Sprintf("create new Consumer successfully after %d retries", i))
		}
		return er2
	})
	if err != nil {
		log.Println(fmt.Sprintf("Fail in creating new Consumer after %d retries", i))
	}
	return &Consumer{
		Consumer: consumer,
		Topics:   []string{c.Topic},
	}, nil
}

func (c *Consumer) Consume(ctx context.Context, handle func(context.Context, []byte, map[string]string)) {
	defer c.Consumer.Close()

	err := c.Consumer.SubscribeTopics(c.Topics, nil)
	if err != nil {
		if c.LogError != nil {
			c.LogError(ctx, fmt.Sprintf("Consume Topic err: %v", err))
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
			handle(ctx, e.Value, h)
		case kafka.PartitionEOF:
			if c.LogInfo != nil {
				c.LogInfo(ctx, fmt.Sprintf("Reached %v", e))
			}
		case kafka.Error:
			if c.LogError != nil {
				c.LogError(ctx, fmt.Sprintf("Error: %v", e))
			}
			run = false
		default:
		}
	}
}
func (c *Consumer) ConsumeValue(ctx context.Context, handle func(context.Context, []byte)) {
	defer c.Consumer.Close()

	err := c.Consumer.SubscribeTopics(c.Topics, nil)
	if err != nil {
		if c.LogError != nil {
			c.LogError(ctx, fmt.Sprintf("Consume Topic err: %v", err))
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
			handle(ctx, e.Value)
		case kafka.PartitionEOF:
			if c.LogInfo != nil {
				c.LogInfo(ctx, fmt.Sprintf("Reached %v", e))
			}
		case kafka.Error:
			if c.LogError != nil {
				c.LogError(ctx, fmt.Sprintf("Error: %v", e))
			}
			run = false
		default:
		}
	}
}
func (c *Consumer) ConsumeMessage(ctx context.Context, handle func(context.Context, *kafka.Message)) {
	defer c.Consumer.Close()

	err := c.Consumer.SubscribeTopics(c.Topics, nil)
	if err != nil {
		if c.LogError != nil {
			c.LogError(ctx, fmt.Sprintf("Consume Topic err: %v", err))
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
			handle(ctx, e)
		case kafka.PartitionEOF:
			if c.LogInfo != nil {
				c.LogInfo(ctx, fmt.Sprintf("Reached %v", e))
			}
		case kafka.Error:
			if c.LogError != nil {
				c.LogError(ctx, fmt.Sprintf("Error: %v", e))
			}
			run = false
		default:
		}
	}
}
func HeaderToMap(headers []kafka.Header) map[string]string {
	attributes := make(map[string]string, 0)
	for _, v := range headers {
		attributes[v.Key] = v.String()
	}
	return attributes
}
