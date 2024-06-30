package rabbitmq

import (
	"context"
	"github.com/streadway/amqp"
	"strings"
	"time"
)

type SimplePublisher struct {
	Channel      *amqp.Channel
	ExchangeName string
	Key          string
	ContentType  string
	Convert      func(context.Context, []byte) ([]byte, error)
}

func NewSimplePublisher(channel *amqp.Channel, exchangeName string, key string, contentType string, options...func(context.Context, []byte)([]byte, error)) (*SimplePublisher, error) {
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
	return &SimplePublisher{Channel: channel, ExchangeName: exchangeName, Key: key, ContentType: contentType, Convert: convert}, nil
}

func NewSimplePublisherByConfig(config PublisherConfig, options...func(context.Context, []byte)([]byte, error)) (*SimplePublisher, error) {
	channel, er1 := NewChannel(config.Url)
	if er1 != nil {
		return nil, er1
	}
	er2 := channel.ExchangeDeclare(config.ExchangeName, config.ExchangeKind, true, config.AutoDelete, false, false, nil)
	if er2 != nil {
		return nil, er2
	}
	return NewSimplePublisher(channel, config.ExchangeName, config.Key, config.ContentType, options...)
}
func (p *SimplePublisher) Put(ctx context.Context, exchangeName string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, exchangeName, data, attributes)
}
func (p *SimplePublisher) Send(ctx context.Context, exchangeName string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, exchangeName, data, attributes)
}
func (p *SimplePublisher) Write(ctx context.Context, exchangeName string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, exchangeName, data, attributes)
}
func (p *SimplePublisher) Produce(ctx context.Context, exchangeName string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, exchangeName, data, attributes)
}
func (p *SimplePublisher) Publish(ctx context.Context, exchangeName string, data []byte, attributes map[string]string) (string, error) {
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
	s := strings.Split(exchangeName, "/")
	if len(s) == 2 {
		err := p.Channel.Publish(s[0], s[1], false, false, msg)
		return "", err
	} else {
		err := p.Channel.Publish(exchangeName, p.Key, false, false, msg)
		return "", err
	}
}
