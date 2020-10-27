package mq

import (
	"context"
	"time"
)

type DefaultProducer struct {
	Producer   Producer
	Retries    []time.Duration
	Goroutines bool
}

func NewProducer(producer Producer, goroutines bool, retries ...time.Duration) *DefaultProducer {
	return &DefaultProducer{Producer: producer, Retries: retries, Goroutines: goroutines}
}
func (c *DefaultProducer) Produce(ctx context.Context, data []byte, attributes *map[string]string) (string, error) {
	if !c.Goroutines {
		return Produce(ctx, c.Producer, data, attributes, c.Retries...)
	} else {
		go Produce(ctx, c.Producer, data, attributes, c.Retries...)
		return "", nil
	}
}
func Produce(ctx context.Context, producer Producer, data []byte, attributes *map[string]string, retries ...time.Duration) (string, error) {
	l := len(retries)
	if l == 0 {
		return producer.Produce(ctx, data, attributes)
	} else {
		return ProduceWithRetries(ctx, producer, data, attributes, retries)
	}
}

func ProduceWithRetries(ctx context.Context, producer Producer, data []byte, attributes *map[string]string, retries []time.Duration) (string, error) {
	id, er1 := producer.Produce(ctx, data, attributes)
	if er1 == nil {
		return id, er1
	}
	i := 0
	err := Retry(ctx, retries, func() (err error) {
		i = i + 1
		id2, er2 := producer.Produce(ctx, data, attributes)
		id = id2
		if er2 == nil {
			s := string(data)
			Infof(ctx, "Produce successfully after %d retries %s", i, s)
		}
		return er2
	})
	if err != nil {
		s := string(data)
		Errorf(ctx, "Failed to produce: %s. Error: %v.", s, err)
	}
	return id, err
}
