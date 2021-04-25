package rabbitmq

import (
	"context"
	"time"

	"github.com/streadway/amqp"
)

type Publisher struct {
	Channel      *amqp.Channel
	ExchangeName string
	Key          string
	ContentType  string
}

func NewPublisher(channel *amqp.Channel, exchangeName string, key string, contentType string) (*Publisher, error) {
	if len(key) == 0 {
		key = "info"
	}
	if len(contentType) == 0 {
		contentType = "text/plain"
	}
	return &Publisher{Channel: channel, ExchangeName: exchangeName, Key: key, ContentType: contentType}, nil
}

func NewPublisherByConfig(config PublisherConfig) (*Publisher, error) {
	channel, er1 := NewChannel(config.Url)
	if er1 != nil {
		return nil, er1
	}
	er2 := channel.ExchangeDeclare(config.ExchangeName, config.ExchangeKind, true, config.AutoDelete, false, false, nil)
	if er2 != nil {
		return nil, er2
	}
	return NewPublisher(channel, config.ExchangeName, config.Key, config.ContentType)
}

func (p *Publisher) Publish(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	opts := MapToTable(attributes)
	msg := amqp.Publishing{
		Headers:      opts,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  p.ContentType,
		Body:         data,
	}
	err := p.Channel.Publish(p.ExchangeName, p.Key, false, false, msg)
	return "", err
}

func MapToTable(attributes map[string]string) amqp.Table {
	opts := amqp.Table{}
	if attributes != nil {
		for k, v := range attributes {
			opts[k] = v
		}
	}
	return opts
}
