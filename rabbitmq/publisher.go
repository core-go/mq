package rabbitmq

import (
	"context"
	"github.com/streadway/amqp"
	"time"
)

type Publisher struct {
	Channel      *amqp.Channel
	ExchangeName string
	Key          string
	ContentType  string
	Convert      func(context.Context, []byte) ([]byte, error)
}

func NewPublisher(channel *amqp.Channel, exchangeName string, key string, contentType string, options...func(context.Context, []byte)([]byte, error)) (*Publisher, error) {
	if len(key) == 0 {
		key = "info"
	}
	if len(contentType) == 0 {
		contentType = "text/plain"
	}
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Publisher{Channel: channel, ExchangeName: exchangeName, Key: key, ContentType: contentType, Convert: convert}, nil
}

func NewPublisherByConfig(config PublisherConfig, options...func(context.Context, []byte)([]byte, error)) (*Publisher, error) {
	channel, er1 := NewChannel(config.Url)
	if er1 != nil {
		return nil, er1
	}
	er2 := channel.ExchangeDeclare(config.ExchangeName, config.ExchangeKind, true, config.AutoDelete, false, false, nil)
	if er2 != nil {
		return nil, er2
	}
	return NewPublisher(channel, config.ExchangeName, config.Key, config.ContentType, options...)
}

func (p *Publisher) Publish(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, data)
		if err != nil {
			return "", err
		}
	}
	opts := MapToTable(attributes)
	msg := amqp.Publishing{
		Headers:      opts,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  p.ContentType,
		Body:         binary,
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
