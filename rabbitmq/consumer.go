package rabbitmq

import (
	"context"
	"github.com/core-go/mq"
	"github.com/streadway/amqp"
)

type Consumer struct {
	Channel      *amqp.Channel
	ExchangeName string
	QueueName    string
	AutoAck      bool
	AckOnConsume bool
	Convert      func(context.Context, []byte) ([]byte, error)
}

func NewConsumer(channel *amqp.Channel, exchangeName string, queueName string, autoAck, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) (*Consumer, error) {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Consumer{Channel: channel, ExchangeName: exchangeName, QueueName: queueName, AutoAck: autoAck, AckOnConsume: ackOnConsume, Convert: convert}, nil
}
func NewConsumerByConfig(config ConsumerConfig, autoAck, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) (*Consumer, error) {
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
	return NewConsumer(channel, config.ExchangeName, queue.Name, autoAck, ackOnConsume, options...)
}

func (c *Consumer) Consume(ctx context.Context, handle func(context.Context, *mq.Message, error) error) {
	delivery, err := c.Channel.Consume(c.QueueName, "", c.AutoAck, false, false, false, nil)
	if err != nil {
		handle(ctx, nil, err)
	} else {
		for msg := range delivery {
			attributes := TableToMap(msg.Headers)

			message := mq.Message{
				Id:         msg.MessageId,
				Data:       msg.Body,
				Attributes: attributes,
				Timestamp:  &msg.Timestamp,
				Raw:        msg,
			}
			if c.AckOnConsume && !c.AutoAck {
				msg.Ack(false)
			}
			if c.Convert == nil {
				handle(ctx, &message, nil)
			} else {
				data, err := c.Convert(ctx, msg.Body)
				if err == nil {
					message.Data = data
				}
				handle(ctx, &message, err)
			}
		}
	}
}
