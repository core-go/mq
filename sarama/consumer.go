package kafka

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"sync"
	"time"
)

type Consumer struct {
	ConsumerGroup sarama.ConsumerGroup
	Topic         []string
	AckOnConsume  bool
	LogError      func(ctx context.Context, msg string)
}

func NewConsumer(consumerGroup sarama.ConsumerGroup, topic []string, logError func(context.Context, string), ackOnConsume bool) (*Consumer, error) {
	return &Consumer{ConsumerGroup: consumerGroup, Topic: topic, AckOnConsume: ackOnConsume, LogError: logError}, nil
}

func NewConsumerByConfig(c ConsumerConfig, logError func(context.Context, string), ackOnConsume bool) (*Consumer, error) {
	algorithm := sarama.SASLTypeSCRAMSHA256
	if c.Client.Algorithm != "" {
		algorithm = c.Client.Algorithm
	}
	conf := sarama.NewConfig()
	config, er1 := GetConfig(c.Brokers, &algorithm, &c.Client, *conf)
	if er1 != nil {
		return nil, er1
	}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	if c.InitialOffsets != nil {
		config.Consumer.Offsets.Initial = *c.InitialOffsets
	}
	//sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	if c.Client.Retry != nil && c.Client.Retry.Retry1 > 0 {
		durations := DurationsFromValue(*c.Client.Retry, "Retry", 9)
		reader, er2 := NewConsumerGroupWithRetryArray(c.Brokers, c.GroupID, config, durations)
		if er2 != nil {
			return nil, er2
		}
		return NewConsumer(*reader, []string{c.Topic}, logError, ackOnConsume)
	} else {
		reader, er2 := sarama.NewConsumerGroup(c.Brokers, c.GroupID, config)
		if er2 != nil {
			return nil, er2
		}
		return NewConsumer(reader, []string{c.Topic}, logError, ackOnConsume)
	}
}
func (c *Consumer) Consume(ctx context.Context, handle func(context.Context, []byte, map[string]string)) {
	readerHandler := &ConsumerHandler{Topic: c.Topic, AckOnConsume: c.AckOnConsume, Handle: handle}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := c.ConsumerGroup.Consume(ctx, c.Topic, readerHandler); err != nil {
				c.LogError(ctx, "Error when read: "+err.Error())
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				c.LogError(ctx, "Error when read: "+ctx.Err().Error())
			}
		}
	}()
	go func() {
		for err := range c.ConsumerGroup.Errors() {
			c.LogError(ctx, "Error when read: "+err.Error())
		}
	}()
	wg.Wait()
	if err := c.ConsumerGroup.Close(); err != nil {
		log.Printf("Error closing client: %v\n", err)
	}
}
func (c *Consumer) ConsumeValue(ctx context.Context, handle func(context.Context, []byte)) {
	newHandle := func(ctx context.Context, value []byte, attrs map[string]string) {
		handle(ctx, value)
	}
	readerHandler := &ConsumerHandler{Topic: c.Topic, AckOnConsume: c.AckOnConsume, Handle: newHandle}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := c.ConsumerGroup.Consume(ctx, c.Topic, readerHandler); err != nil {
				c.LogError(ctx, "Error when read: "+err.Error())
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				c.LogError(ctx, "Error when read: "+ctx.Err().Error())
			}
		}
	}()
	go func() {
		for err := range c.ConsumerGroup.Errors() {
			c.LogError(ctx, "Error when read: "+err.Error())
		}
	}()
	wg.Wait()
	if err := c.ConsumerGroup.Close(); err != nil {
		log.Printf("Error closing client: %v\n", err)
	}
}
func NewConsumerGroup(addrs []string, groupID string, config *sarama.Config, retries ...time.Duration) (*sarama.ConsumerGroup, error) {
	if len(retries) == 0 {
		reader, err := sarama.NewConsumerGroup(addrs, groupID, config)
		if err != nil {
			return nil, err
		}
		return &reader, err
	} else {
		return NewConsumerGroupWithRetryArray(addrs, groupID, config, retries)
	}
}
func NewConsumerGroupWithRetryArray(addrs []string, groupID string, config *sarama.Config, retries []time.Duration) (*sarama.ConsumerGroup, error) {
	r, er1 := sarama.NewConsumerGroup(addrs, groupID, config)
	if er1 == nil {
		return &r, er1
	}
	i := 0
	err := Retry(retries, func() (err error) {
		i = i + 1
		r2, er2 := sarama.NewConsumerGroup(addrs, groupID, config)
		r = r2
		if er2 == nil {
			log.Println(fmt.Sprintf("New ConsumerGroup after successfully %d retries", i))
		}
		return er2
	})
	if err != nil {
		log.Println(fmt.Sprintf("Failed to after successfully %d retries", i))
	}
	return &r, err
}
