package activemq

import (
	"context"
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
)

type Subscriber struct {
	Conn         *stomp.Conn
	Subscription *stomp.Subscription
	AckMode      stomp.AckMode
	LogError     func(ctx context.Context, msg string)
	AckOnConsume bool
}

func NewSubscriber(client *stomp.Conn, destinationName string, subscriptionName string, ackMode stomp.AckMode, logError func(ctx context.Context, msg string), ackOnConsume bool) (*Subscriber, error) {
	des := destinationName + "::" + subscriptionName
	subscription, err := client.Subscribe(des, ackMode,
		stomp.SubscribeOpt.Header("subscription-type", "ANYCAST"),
	)
	if err != nil {
		return nil, err
	}
	return &Subscriber{Conn: client, Subscription: subscription, AckMode: ackMode, LogError: logError, AckOnConsume: ackOnConsume}, nil
}

func NewSubscriberByConfig(c Config, ackMode stomp.AckMode, logError func(ctx context.Context, msg string), ackOnConsume bool) (*Subscriber, error) {
	client, err := NewConnWithHeartBeat(c.UserName, c.Password, c.Addr, 5*time.Second, -1)
	if err != nil {
		return nil, err
	}
	return NewSubscriber(client, c.DestinationName, c.SubscriptionName, ackMode, logError, ackOnConsume)
}
func (c *Subscriber) SubscribeMessage(ctx context.Context, handle func(context.Context, *stomp.Message)) {
	for msg := range c.Subscription.C {
		if msg.Err != nil {
			c.LogError(ctx, "Error when subscribe: "+msg.Err.Error())
		} else {
			if c.AckOnConsume && c.AckMode != stomp.AckAuto {
				c.Conn.Ack(msg)
			}
			handle(ctx, msg)
		}

	}
}
func (c *Subscriber) Subscribe(ctx context.Context, handle func(context.Context, []byte, map[string]string)) {
	for msg := range c.Subscription.C {
		if msg.Err != nil {
			c.LogError(ctx, "Error when subscribe: "+msg.Err.Error())
		} else {
			if c.AckOnConsume && c.AckMode != stomp.AckAuto {
				c.Conn.Ack(msg)
			}
			attributes := HeaderToMap(msg.Header)
			handle(ctx, msg.Body, attributes)
		}

	}
}
func (c *Subscriber) SubscribeBody(ctx context.Context, handle func(context.Context, []byte)) {
	for msg := range c.Subscription.C {
		if msg.Err != nil {
			c.LogError(ctx, "Error when subscribe: "+msg.Err.Error())
		} else {
			if c.AckOnConsume && c.AckMode != stomp.AckAuto {
				c.Conn.Ack(msg)
			}
			handle(ctx, msg.Body)
		}
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
