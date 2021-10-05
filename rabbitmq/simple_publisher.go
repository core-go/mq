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
}

func NewSimplePublisher(channel *amqp.Channel, exchangeName string, key string, contentType string) (*SimplePublisher, error) {
	if len(key) == 0 {
		key = "info"
	}
	if len(contentType) == 0 {
		contentType = "text/plain"
	}
	return &SimplePublisher{Channel: channel, ExchangeName: exchangeName, Key: key, ContentType: contentType}, nil
}

func NewSimplePublisherByConfig(config PublisherConfig) (*SimplePublisher, error) {
	channel, er1 := NewChannel(config.Url)
	if er1 != nil {
		return nil, er1
	}
	er2 := channel.ExchangeDeclare(config.ExchangeName, config.ExchangeKind, true, config.AutoDelete, false, false, nil)
	if er2 != nil {
		return nil, er2
	}
	return NewSimplePublisher(channel, config.ExchangeName, config.Key, config.ContentType)
}

func (p *SimplePublisher) Publish(ctx context.Context, exchangeName string, data []byte, attributes map[string]string) (string, error) {
	opts := MapToTable(attributes)
	msg := amqp.Publishing{
		Headers:      opts,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  p.ContentType,
		Body:         data,
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
