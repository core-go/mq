package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"sync"
	"time"
)

type SimpleReader struct {
	ConsumerGroup sarama.ConsumerGroup
	Topic         []string
	AckOnConsume  bool
}

func NewSimpleReader(consumerGroup sarama.ConsumerGroup, topic []string, ackOnConsume bool) (*SimpleReader, error) {
	return &SimpleReader{ConsumerGroup: consumerGroup, Topic: topic, AckOnConsume: ackOnConsume}, nil
}

func NewSimpleReaderByConfig(c ReaderConfig, ackOnConsume bool) (*SimpleReader, error) {
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
		reader, er2 := NewReaderGroupWithRetryArray(c.Brokers, c.GroupID, config, durations)
		if er2 != nil {
			return nil, er2
		}
		return NewSimpleReader(*reader, []string{c.Topic}, ackOnConsume)
	} else {
		reader, er2 := sarama.NewConsumerGroup(c.Brokers, c.GroupID, config)
		if er2 != nil {
			return nil, er2
		}
		return NewSimpleReader(reader, []string{c.Topic}, ackOnConsume)
	}
}
func NewReaderGroup(addrs []string, groupID string, config *sarama.Config, retries ...time.Duration) (*sarama.ConsumerGroup, error) {
	if len(retries) == 0 {
		reader, err := sarama.NewConsumerGroup(addrs, groupID, config)
		if err != nil {
			return nil, err
		}
		return &reader, err
	} else {
		return NewReaderGroupWithRetryArray(addrs, groupID, config, retries)
	}
}
func NewReaderGroupWithRetryArray(addrs []string, groupID string, config *sarama.Config, retries []time.Duration) (*sarama.ConsumerGroup, error) {
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
func (c *SimpleReader) Read(ctx context.Context, handle func(context.Context, []byte, map[string]string, error) error) {
	readerHandler := &SimpleReaderHandler{Topic: c.Topic, AckOnConsume: c.AckOnConsume, Handle: handle}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := c.ConsumerGroup.Consume(ctx, c.Topic, readerHandler); err != nil {
				handle(ctx, nil, nil, err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				handle(ctx, nil, nil, ctx.Err())
			}
		}
	}()
	go func() {
		for err := range c.ConsumerGroup.Errors() {
			handle(ctx, nil, nil, err)
		}
	}()
	wg.Wait()
	if err := c.ConsumerGroup.Close(); err != nil {
		log.Printf("Error closing client: %v\n", err)
	}
}
