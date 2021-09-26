package amq

import (
	"context"
	"github.com/go-stomp/stomp"
	"github.com/go-stomp/stomp/frame"
	"time"
)

type SimpleSubscriber struct {
	Conn         *stomp.Conn
	Subscription *stomp.Subscription
	AckMode      stomp.AckMode
	AckOnConsume bool
}

func NewSimpleSubscriber(client *stomp.Conn, destinationName string, subscriptionName string, ackMode stomp.AckMode, ackOnConsume bool) (*SimpleSubscriber, error) {
	des := destinationName + "::" + subscriptionName
	subscription, err := client.Subscribe(des, ackMode,
		stomp.SubscribeOpt.Header("subscription-type", "ANYCAST"),
	)
	if err != nil {
		return nil, err
	}
	return &SimpleSubscriber{Conn: client, Subscription: subscription, AckMode: ackMode, AckOnConsume: ackOnConsume}, nil
}

func NewSimpleSubscriberByConfig(c Config, ackMode stomp.AckMode, ackOnConsume bool) (*SimpleSubscriber, error) {
	client, err := NewConnWithHeartBeat(c.UserName, c.Password, c.Addr, 5*time.Second, -1)
	if err != nil {
		return nil, err
	}
	return NewSimpleSubscriber(client, c.DestinationName, c.SubscriptionName, ackMode, ackOnConsume)
}

func (c *SimpleSubscriber) Subscribe(ctx context.Context, handle func(context.Context, []byte, map[string]string, error) error) {
	for msg := range c.Subscription.C {
		attributes := HeaderToMap(msg.Header)
		if c.AckOnConsume && c.AckMode != stomp.AckAuto {
			c.Conn.Ack(msg)
		}
		handle(ctx, msg.Body, attributes, nil)
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
