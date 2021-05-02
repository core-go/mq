package kafka

import (
	"github.com/segmentio/kafka-go"
	"time"
)

func NewKafkaReader(c ReaderConfig, dialer *kafka.Dialer) *kafka.Reader {
	c2 := kafka.ReaderConfig{
		Brokers: c.Brokers,
		GroupID: c.GroupID,
		Topic:   c.Topic,
		Dialer:  dialer,
	}
	if c.CommitInterval != nil {
		c2.CommitInterval = time.Duration(*c.CommitInterval) * time.Nanosecond
	}
	if c.MinBytes != nil && *c.MinBytes >= 0 {
		c2.MinBytes = *c.MinBytes
	}
	if c.MaxBytes > 0 {
		c2.MaxBytes = c.MaxBytes
	}
	return kafka.NewReader(c2)
}

func HeaderToMap(headers []kafka.Header) map[string]string {
	attributes := make(map[string]string, 0)
	for i := range headers {
		attributes[headers[i].Key] = string(headers[i].Value)
	}
	return attributes
}
