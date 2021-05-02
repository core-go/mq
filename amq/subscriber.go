package amq

import (
	"context"
	"github.com/core-go/mq"
	"github.com/go-stomp/stomp"
	"github.com/go-stomp/stomp/frame"
	"time"
)

type Subscriber struct {
	Conn         *stomp.Conn
	Subscription *stomp.Subscription
	AckMode      stomp.AckMode
	AckOnConsume bool
}

func NewSubscriber(client *stomp.Conn, destinationName string, subscriptionName string, ackMode stomp.AckMode, ackOnConsume bool) (*Subscriber, error) {
	des := destinationName + "::" + subscriptionName
	subscription, err := client.Subscribe(des, ackMode,
		stomp.SubscribeOpt.Header("subscription-type", "ANYCAST"),
	)
	if err != nil {
		return nil, err
	}
	return &Subscriber{Conn: client, Subscription: subscription, AckMode: ackMode, AckOnConsume: ackOnConsume}, nil
}

func NewSubscriberByConfig(c Config, ackMode stomp.AckMode, ackOnConsume bool) (*Subscriber, error) {
	client, err := NewConnWithHeartBeat(c.UserName, c.Password, c.Addr, 5*time.Second, -1)
	if err != nil {
		return nil, err
	}
	return NewSubscriber(client, c.DestinationName, c.SubscriptionName, ackMode, ackOnConsume)
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
		handle(ctx, &message, nil)
	}
}

func HeaderToMap(header *frame.Header) map[string]string {
	attributes := make(map[string]string, 0)
	for i := 0; i < header.Len(); i++ {
		key, value := header.GetAt(i)
		attributes[key] = value
	}
	return attributes
}
