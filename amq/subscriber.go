package amq

import (
	"context"
	"github.com/core-go/mq"
	"github.com/go-stomp/stomp"
	"time"
)

type Subscriber struct {
	Conn         *stomp.Conn
	Subscription *stomp.Subscription
	AckMode      stomp.AckMode
	AckOnConsume bool
	Convert      func(context.Context, []byte) ([]byte, error)
}

func NewSubscriber(client *stomp.Conn, destinationName string, subscriptionName string, ackMode stomp.AckMode, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) (*Subscriber, error) {
	des := destinationName + "::" + subscriptionName
	subscription, err := client.Subscribe(des, ackMode,
		stomp.SubscribeOpt.Header("subscription-type", "ANYCAST"),
	)
	if err != nil {
		return nil, err
	}
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Subscriber{Conn: client, Subscription: subscription, AckMode: ackMode, AckOnConsume: ackOnConsume, Convert: convert}, nil
}

func NewSubscriberByConfig(c Config, ackMode stomp.AckMode, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) (*Subscriber, error) {
	client, err := NewConnWithHeartBeat(c.UserName, c.Password, c.Addr, 5*time.Second, -1)
	if err != nil {
		return nil, err
	}
	return NewSubscriber(client, c.DestinationName, c.SubscriptionName, ackMode, ackOnConsume, options...)
}

func (c *Subscriber) Subscribe(ctx context.Context, handle func(context.Context, *mq.Message, error) error) {
	for msg := range c.Subscription.C {
		attributes := HeaderToMap(msg.Header)
		message := mq.Message{
			Data:       msg.Body,
			Attributes: attributes,
			Raw:        msg,
		}
		if c.AckOnConsume && c.AckMode != stomp.AckAuto {
			c.Conn.Ack(msg)
		}
		if c.Convert == nil {
			handle(ctx, &message, nil)
		} else {
			data, err := c.Convert(ctx, msg.Body)
			if err == nil {
				message.Data = data
			}
			handle(ctx, &message, nil)
		}
	}
}
