package mq

import (
	"context"
	"github.com/sirupsen/logrus"
	"time"
)

type RetryProducer struct {
	Producer   Producer
	Retries    []time.Duration
}
func NewRetryProducer(producer Producer, retries []time.Duration) *RetryProducer {
	return &RetryProducer{Producer: producer, Retries: retries}
}
func (c *RetryProducer) Produce(ctx context.Context, data []byte, attributes *map[string]string) (string, error) {
	id, er1 := c.Producer.Produce(ctx, data, attributes)
	if er1 == nil {
		return id, er1
	}
	i := 0
	err := Retry(c.Retries, func() (err error) {
		i = i + 1
		id2, er2 := c.Producer.Produce(ctx, data, attributes)
		id = id2
		if er2 == nil {
			s := string(data)
			logrus.Infof("Produce successfully after %d retries %s", i, s)
		}
		return er2
	})
	if err != nil {
		s := string(data)
		logrus.Errorf("Failed to produce: %s. Error: %v.", s, err)
	}
	return id, err
}
