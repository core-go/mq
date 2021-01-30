package mq

import (
	"context"
	"fmt"
	"time"
)

type DefaultProducer struct {
	Producer   Producer
	Retries    []time.Duration
	Log        func(context.Context, string)
	Goroutines bool
}

func NewProducerByConfig(producer Producer, goroutines bool, log func(context.Context, string), c *RetryConfig) *DefaultProducer {
	if c == nil {
		return &DefaultProducer{Producer: producer, Log: log, Goroutines: goroutines}
	} else {
		retries := DurationsFromValue(*c, "Retry", 20)
		if len(retries) == 0 {
			return &DefaultProducer{Producer: producer, Log: log, Goroutines: goroutines}
		}
		return &DefaultProducer{Producer: producer, Log: log, Retries: retries, Goroutines: goroutines}
	}
}
func NewProducer(producer Producer, goroutines bool, log func(context.Context, string), retries ...time.Duration) *DefaultProducer {
	return &DefaultProducer{Producer: producer, Log: log, Retries: retries, Goroutines: goroutines}
}
func (c *DefaultProducer) Produce(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	if !c.Goroutines {
		return Produce(ctx, c.Producer, data, attributes, c.Log, c.Retries...)
	} else {
		go Produce(ctx, c.Producer, data, attributes, c.Log, c.Retries...)
		return "", nil
	}
}
func Produce(ctx context.Context, producer Producer, data []byte, attributes map[string]string, log func(context.Context, string), retries ...time.Duration) (string, error) {
	l := len(retries)
	if l == 0 {
		return producer.Produce(ctx, data, attributes)
	} else {
		return ProduceWithRetries(ctx, producer, data, attributes, retries, log)
	}
}

func ProduceWithRetries(ctx context.Context, producer Producer, data []byte, attributes map[string]string, retries []time.Duration, log func(context.Context, string)) (string, error) {
	id, er1 := producer.Produce(ctx, data, attributes)
	if er1 == nil {
		return id, er1
	}
	i := 0
	err := Retry(ctx, retries, func() (err error) {
		i = i + 1
		id2, er2 := producer.Produce(ctx, data, attributes)
		id = id2
		if er2 == nil && log != nil {
			log(ctx, fmt.Sprintf("Produce successfully after %d retries %s", i, data))
		}
		return er2
	}, log)
	if err != nil && log != nil {
		log(ctx, fmt.Sprintf("Failed to produce after %d retries: %s. Error: %s.", len(retries), data, err.Error()))
	}
	return id, err
}
