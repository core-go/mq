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
	}
)

func NewConsumerByConfig(c ConsumerConfig) (*Consumer, error) {
	if c.Client.Retry != nil && c.Client.Retry.Retry1 > 0 {
		durations := DurationsFromValue(*c.Client.Retry, "Retry", 9)
		return NewConsumerByConfigAndRetryArray(c, durations)
	} else {
		consumer, err := NewKafkaConsumerByConfig(c)
		if err != nil {
			fmt.Printf("Failed to create Consumer: %s\n", err)
			return nil, err
		}
		return &Consumer{
			Consumer: consumer,
			Topics:   []string{c.Topic},
		}, nil
	}
}
func NewConsumer(consumer *kafka.Consumer, topics []string) *Consumer {
	return &Consumer{Consumer: consumer, Topics: topics}
}
func NewConsumerByConfigAndRetries(c ConsumerConfig, retries ...time.Duration) (*Consumer, error) {
	if len(retries) == 0 {
		return NewConsumerByConfig(c)
	} else {
		return NewConsumerByConfigAndRetryArray(c, retries)
	}
}

func NewConsumerByConfigAndRetryArray(c ConsumerConfig, retries []time.Duration) (*Consumer, error) {
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

func (c *Consumer) Consume(ctx context.Context, handle func(context.Context, []byte, map[string]string, error) error) {
	Consume(ctx, c.Consumer, c.Topics, handle)
}

func Consume(ctx context.Context, consumer *kafka.Consumer, topics []string, handle func(context.Context, []byte, map[string]string, error) error) {
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
			fmt.Printf("%% Message on %s:\n%s\n",
				e.TopicPartition, string(e.Value))
			handle(ctx, e.Value, nil, nil)
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