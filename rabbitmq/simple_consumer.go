package rabbitmq

import (
	"context"
	"github.com/streadway/amqp"
)

type SimpleConsumer struct {
	Channel      *amqp.Channel
	ExchangeName string
	QueueName    string
	AutoAck      bool
	AckOnConsume bool
	Convert      func(context.Context, []byte) ([]byte, error)
}

func NewSimpleConsumer(channel *amqp.Channel, exchangeName string, queueName string, autoAck, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) (*SimpleConsumer, error) {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &SimpleConsumer{Channel: channel, ExchangeName: exchangeName, QueueName: queueName, AutoAck: autoAck, AckOnConsume: ackOnConsume, Convert: convert}, nil
}
func NewSimpleConsumerByConfig(config ConsumerConfig, autoAck, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) (*SimpleConsumer, error) {
	channel, er1 := NewChannel(config.Url)
	if er1 != nil {
		return nil, er1
	}
	err := channel.ExchangeDeclare(config.ExchangeName, config.ExchangeKind, true, config.AutoDelete, false, false, nil)
	if err != nil {
		return nil, err
	}
	queue, err := channel.QueueDeclare(config.QueueName, false, false, true, false, nil)
	if err != nil {
		return nil, err
	}
	return NewSimpleConsumer(channel, config.ExchangeName, queue.Name, autoAck, ackOnConsume, options...)
}

func (c *SimpleConsumer) Consume(ctx context.Context, handle func(context.Context, []byte, map[string]string, error) error) {
	delivery, err := c.Channel.Consume(c.QueueName, "", c.AutoAck, false, false, false, nil)
	if err != nil {
		handle(ctx, nil, nil, err)
	} else {
		for msg := range delivery {
			attributes := TableToMap(msg.Headers)
			if c.AckOnConsume && !c.AutoAck {
				msg.Ack(false)
			}
			if c.Convert == nil {
				handle(ctx, msg.Body, attributes, nil)
			} else {
				data, err := c.Convert(ctx, msg.Body)
				handle(ctx, data, attributes, err)
			}
		}
	}
}

func TableToMap(header amqp.Table) map[string]string {
	attributes := make(map[string]string, 0)
	for k, v := range header {
		attributes[k] = v.(string)
	}
	return attributes
}
