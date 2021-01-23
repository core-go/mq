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

func NewProducer(producer Producer, goroutines bool, log func(context.Context, string), retries ...time.Duration) *DefaultProducer {
	return &DefaultProducer{Producer: producer, Log: log, Retries: retries, Goroutines: goroutines}
}
func (c *DefaultProducer) Produce(ctx context.Context, data []byte, attributes *map[string]string) (string, error) {
	if !c.Goroutines {
		return Produce(ctx, c.Producer, data, attributes, c.Log, c.Retries...)
	} else {
		go Produce(ctx, c.Producer, data, attributes, c.Log, c.Retries...)
		return "", nil
	}
}
func Produce(ctx context.Context, producer Producer, data []byte, attributes *map[string]string, log func(context.Context, string), retries ...time.Duration) (string, error) {
	l := len(retries)
	if l == 0 {
		return producer.Produce(ctx, data, attributes)
	} else {
		return ProduceWithRetries(ctx, producer, data, attributes, retries, log)
	}
}

func ProduceWithRetries(ctx context.Context, producer Producer, data []byte, attributes *map[string]string, retries []time.Duration, log func(context.Context, string)) (string, error) {
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
			s := string(data)
			m := fmt.Sprintf("Produce successfully after %d retries %s", i, s)
			log(ctx, m)
		}
		return er2
	})
	if err != nil && log != nil {
		s := string(data)
		m := fmt.Sprintf("Failed to produce: %s. Error: %s.", s, err.Error())
		log(ctx, m)
	}
	return id, err
}
