package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/core-go/mq"
	"log"
	"sync"
)

type Reader struct {
	ConsumerGroup sarama.ConsumerGroup
	Topic         []string
	AckOnConsume  bool
	Convert       func(context.Context, []byte) ([]byte, error)
}

func NewReader(consumerGroup sarama.ConsumerGroup, topic []string, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) (*Reader, error) {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Reader{ConsumerGroup: consumerGroup, Topic: topic, AckOnConsume: ackOnConsume, Convert: convert}, nil
}

func NewReaderByConfig(c ReaderConfig, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) (*Reader, error) {
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
		return NewReader(*reader, []string{c.Topic}, ackOnConsume, options...)
	} else {
		reader, er2 := sarama.NewConsumerGroup(c.Brokers, c.GroupID, config)
		if er2 != nil {
			return nil, er2
		}
		return NewReader(reader, []string{c.Topic}, ackOnConsume, options...)
	}
}
func (c *Reader) Read(ctx context.Context, handle func(context.Context, *mq.Message, error) error) {
	readerHandler := &ReaderHandler{Topic: c.Topic, AckOnConsume: c.AckOnConsume, Handle: handle, Convert: c.Convert}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := c.ConsumerGroup.Consume(ctx, c.Topic, readerHandler); err != nil {
				handle(ctx, nil, err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				handle(ctx, nil, ctx.Err())
			}
		}
	}()
	go func() {
		for err := range c.ConsumerGroup.Errors() {
			handle(ctx, nil, err)
		}
	}()
	wg.Wait()
	if err := c.ConsumerGroup.Close(); err != nil {
		log.Printf("Error closing client: %v\n", err)
	}
}
