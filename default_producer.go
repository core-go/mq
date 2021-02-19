package mq

import (
	"context"
	"fmt"
	"time"
)

type DefaultProducer struct {
	produce    func(ctx context.Context, data []byte, attributes map[string]string) (string, error)
	Retries    []time.Duration
	Log        func(context.Context, string)
	Goroutines bool
}

func NewProducerByConfig(produce func(context.Context, []byte, map[string]string) (string, error), goroutines bool, log func(context.Context, string), c *RetryConfig) *DefaultProducer {
	if c == nil {
		return &DefaultProducer{produce: produce, Log: log, Goroutines: goroutines}
	} else {
		retries := DurationsFromValue(*c, "Retry", 20)
		if len(retries) == 0 {
			return &DefaultProducer{produce: produce, Log: log, Goroutines: goroutines}
		}
		return &DefaultProducer{produce: produce, Log: log, Retries: retries, Goroutines: goroutines}
	}
}
func NewProducer(produce func(context.Context, []byte, map[string]string) (string, error), goroutines bool, log func(context.Context, string), retries ...time.Duration) *DefaultProducer {
	return &DefaultProducer{produce: produce, Log: log, Retries: retries, Goroutines: goroutines}
}
func (c *DefaultProducer) Produce(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	if !c.Goroutines {
		return Produce(ctx, c.produce, data, attributes, c.Log, c.Retries...)
	} else {
		go Produce(ctx, c.produce, data, attributes, c.Log, c.Retries...)
		return "", nil
	}
}
func Produce(ctx context.Context, produce func(context.Context, []byte, map[string]string) (string, error), data []byte, attributes map[string]string, log func(context.Context, string), retries ...time.Duration) (string, error) {
	l := len(retries)
	if l == 0 {
		return produce(ctx, data, attributes)
	} else {
		return ProduceWithRetries(ctx, produce, data, attributes, retries, log)
	}
}

func ProduceWithRetries(ctx context.Context, produce func(context.Context, []byte, map[string]string) (string, error), data []byte, attributes map[string]string, retries []time.Duration, log func(context.Context, string)) (string, error) {
	id, er1 := produce(ctx, data, attributes)
	if er1 == nil {
		return id, er1
	}
	i := 0
	err := Retry(ctx, retries, func() (err error) {
		i = i + 1
		id2, er2 := produce(ctx, data, attributes)
		id = id2
		if er2 == nil && log != nil {
			log(ctx, fmt.Sprintf("Produce successfully after %d retries %s", i, data))
		}
		return er2
	}, log)
	if err != nil && log != nil {
		log(ctx, fmt.Sprintf("Failed to Produce after %d retries: %s. Error: %s.", len(retries), data, err.Error()))
	}
	return id, err
}
