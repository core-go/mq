package rabbitmq

import (
	"context"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangePublisher struct {
	Channel      *amqp.Channel
	ExchangeName string
	Key          string
	ContentType  string
}

func NewExchangePublisher(channel *amqp.Channel, exchangeName string, key string, contentType string) (*ExchangePublisher, error) {
	if len(key) == 0 {
		key = "info"
	}
	if len(contentType) == 0 {
		contentType = "text/plain"
	}
	return &ExchangePublisher{Channel: channel, ExchangeName: exchangeName, Key: key, ContentType: contentType}, nil
}

func NewExchangePublisherByConfig(config PublisherConfig) (*ExchangePublisher, error) {
	channel, er1 := NewChannel(config.Url)
	if er1 != nil {
		return nil, er1
	}
	er2 := channel.ExchangeDeclare(config.ExchangeName, config.ExchangeKind, true, config.AutoDelete, false, false, nil)
	if er2 != nil {
		return nil, er2
	}
	return NewExchangePublisher(channel, config.ExchangeName, config.Key, config.ContentType)
}
func (p *ExchangePublisher) Publish(ctx context.Context, exchangeName string, data []byte, attributes map[string]string) error {
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
		return p.Channel.Publish(s[0], s[1], false, false, msg)
	} else {
		return p.Channel.Publish(exchangeName, p.Key, false, false, msg)
	}
}
func (p *ExchangePublisher) PublishBody(ctx context.Context, exchangeName string, data []byte) error {
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  p.ContentType,
		Body:         data,
	}
	s := strings.Split(exchangeName, "/")
	if len(s) == 2 {
		return p.Channel.Publish(s[0], s[1], false, false, msg)
	} else {
		return p.Channel.Publish(exchangeName, p.Key, false, false, msg)
	}
}
func (p *ExchangePublisher) PublishMessage(exchangeName string, msg amqp.Publishing) error {
	s := strings.Split(exchangeName, "/")
	if len(s) == 2 {
		return p.Channel.Publish(s[0], s[1], false, false, msg)
	} else {
		return p.Channel.Publish(exchangeName, p.Key, false, false, msg)
	}
}
