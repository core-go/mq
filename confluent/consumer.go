package kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
	"time"
)

type (
	Consumer struct {
		Consumer *kafka.Consumer
		Topics   []string
		Convert  func(context.Context, []byte) ([]byte, error)
	}
)

func NewConsumerByConfig(c ConsumerConfig, options ...func(context.Context, []byte) ([]byte, error)) (*Consumer, error) {
	if c.Client.Retry != nil && c.Client.Retry.Retry1 > 0 {
		durations := DurationsFromValue(*c.Client.Retry, "Retry", 9)
		return NewConsumerByConfigAndRetryArray(c, durations)
	} else {
		consumer, err := NewKafkaConsumerByConfig(c)
		if err != nil {
			fmt.Printf("Failed to create Consumer: %s\n", err)
			return nil, err
		}
		var convert func(context.Context, []byte) ([]byte, error)
		if len(options) > 0 {
			convert = options[0]
		}
		return &Consumer{
			Consumer: consumer,
			Topics:   []string{c.Topic},
			Convert:  convert,
		}, nil
	}
}
func NewConsumerByConfigMap(conf kafka.ConfigMap, topics []string, options ...func(context.Context, []byte) ([]byte, error)) (*Consumer, error) {
	consumer, err := kafka.NewConsumer(&conf)
	if err != nil {
		fmt.Printf("Failed to create Consumer: %s\n", err)
		return nil, err
	}
	var convert func(context.Context, []byte) ([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Consumer{
		Consumer: consumer,
		Topics:   topics,
		Convert:  convert,
	}, nil
}
func NewConsumer(consumer *kafka.Consumer, topics []string, options ...func(context.Context, []byte) ([]byte, error)) *Consumer {
	var convert func(context.Context, []byte) ([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Consumer{Consumer: consumer, Topics: topics, Convert: convert}
}
func NewConsumerByConfigAndRetries(c ConsumerConfig, convert func(context.Context, []byte) ([]byte, error), retries ...time.Duration) (*Consumer, error) {
	if len(retries) == 0 {
		return NewConsumerByConfig(c, convert)
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
	var convert func(context.Context, []byte) ([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Consumer{
		Consumer: consumer,
		Topics:   []string{c.Topic},
		Convert:  convert,
	}, nil
}

func (c *Consumer) Consume(ctx context.Context, handle func(context.Context, []byte, map[string]string, error) error) {
	Consume(ctx, c.Consumer, c.Topics, handle, c.Convert)
}

func Consume(ctx context.Context, consumer *kafka.Consumer, topics []string, handle func(context.Context, []byte, map[string]string, error) error, convert func(context.Context, []byte)([]byte, error)) {
	defer consumer.Close()

	err := consumer.SubscribeTopics(topics, nil)
	if err != nil {
		fmt.Printf("%% Subscribe Topic err: %v\n", err)
		return
	}

	run := true
	for run == true {
		ev := consumer.Poll(0)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
			h := HeaderToMap(e.Headers)
			if convert == nil {
				handle(ctx, e.Value, h, nil)
			} else {
				data, err := convert(ctx, e.Value)
				handle(ctx, data, h, err)
			}
		case kafka.PartitionEOF:
			fmt.Printf("%% Reached %v\n", e)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			handle(ctx, nil, nil, e)
			run = false
		default:
		}
	}
}
func HeaderToMap(headers []kafka.Header) map[string]string  {
	attributes := make(map[string]string, 0)
	for _, v := range headers {
		attributes[v.Key] = v.String()
	}
	return attributes
}
