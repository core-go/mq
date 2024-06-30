package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	Channel      *amqp.Channel
	ExchangeName string
	QueueName    string
	AutoAck      bool
	AckOnConsume bool
	LogError     func(ctx context.Context, msg string)
}

func NewConsumer(channel *amqp.Channel, exchangeName string, queueName string, autoAck, ackOnConsume bool, logError func(ctx context.Context, msg string)) (*Consumer, error) {
	return &Consumer{Channel: channel, ExchangeName: exchangeName, QueueName: queueName, AutoAck: autoAck, AckOnConsume: ackOnConsume, LogError: logError}, nil
}
func NewConsumerByConfig(config ConsumerConfig, autoAck, ackOnConsume bool, logError func(ctx context.Context, msg string)) (*Consumer, error) {
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
	return NewConsumer(channel, config.ExchangeName, queue.Name, autoAck, ackOnConsume, logError)
}

func (c *Consumer) Consume(ctx context.Context, handle func(context.Context, []byte, map[string]string)) {
	delivery, err := c.Channel.Consume(c.QueueName, "", c.AutoAck, false, false, false, nil)
	if err != nil {
		c.LogError(ctx, "Error when consume: "+err.Error())
	} else {
		for msg := range delivery {
			attributes := TableToMap(msg.Headers)
			if c.AckOnConsume && !c.AutoAck {
				msg.Ack(false)
			}
			handle(ctx, msg.Body, attributes)
		}
	}
}
func (c *Consumer) ConsumeBody(ctx context.Context, handle func(context.Context, []byte)) {
	delivery, err := c.Channel.Consume(c.QueueName, "", c.AutoAck, false, false, false, nil)
	if err != nil {
		c.LogError(ctx, "Error when consume: "+err.Error())
	} else {
		for msg := range delivery {
			if c.AckOnConsume && !c.AutoAck {
				msg.Ack(false)
			}
			handle(ctx, msg.Body)
		}
	}
}
func (c *Consumer) ConsumeDelivery(ctx context.Context, handle func(context.Context, amqp.Delivery)) {
	delivery, err := c.Channel.Consume(c.QueueName, "", c.AutoAck, false, false, false, nil)
	if err != nil {
		c.LogError(ctx, "Error when consume: "+err.Error())
	} else {
		for msg := range delivery {
			if c.AckOnConsume && !c.AutoAck {
				msg.Ack(false)
			}
			handle(ctx, msg)
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
