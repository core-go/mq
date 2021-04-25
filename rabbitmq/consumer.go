package rabbitmq

import (
	"context"

	"github.com/common-go/mq"
	"github.com/streadway/amqp"
)

type Consumer struct {
	Channel      *amqp.Channel
	ExchangeName string
	QueueName    string
	AutoAck      bool
	AckOnConsume bool
}

func NewConsumer(channel *amqp.Channel, exchangeName string, queueName string, autoAck, ackOnConsume bool) (*Consumer, error) {
	return &Consumer{Channel: channel, ExchangeName: exchangeName, QueueName: queueName, AutoAck: autoAck, AckOnConsume: ackOnConsume}, nil
}
func NewConsumerByConfig(config ConsumerConfig, autoAck, ackOnConsume bool) (*Consumer, error) {
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
	return NewConsumer(channel, config.ExchangeName, queue.Name, autoAck, ackOnConsume)
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
				Raw:        msg,
			}
			if c.AckOnConsume && !c.AutoAck {
				msg.Ack(false)
			}
			handle(ctx, &message, nil)
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
