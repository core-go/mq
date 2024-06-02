package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
)

type Subscriber struct {
	Client       *pubsub.Client
	Subscription *pubsub.Subscription
	LogError     func(ctx context.Context, msg string)
	AckOnConsume bool
	ID           string
}

func NewSubscriber(client *pubsub.Client, subscriptionId string, c SubscriptionConfig, logError func(context.Context, string), ackOnConsume bool, id string) *Subscriber {
	subscription := client.Subscription(subscriptionId)
	return &Subscriber{Client: client, Subscription: ConfigureSubscription(subscription, c), LogError: logError, AckOnConsume: ackOnConsume, ID: id}
}

func NewSubscriberByConfig(ctx context.Context, c SubscriberConfig, logError func(context.Context, string), ackOnConsume bool) (*Subscriber, error) {
	if c.Retry.Retry1 <= 0 {
		client, err := NewPubSubClient(ctx, []byte(c.Client.Credentials), c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewSubscriber(client, c.SubscriptionId, c.SubscriptionConfig, logError, ackOnConsume, ""), nil
	} else {
		durations := DurationsFromValue(c.Retry, "Retry", 9)
		client, err := NewPubSubClientWithRetries(ctx, []byte(c.Client.Credentials), durations, c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewSubscriber(client, c.SubscriptionId, c.SubscriptionConfig, logError, ackOnConsume, ""), nil
	}
}

func (c *Subscriber) SubscribeMessage(ctx context.Context, handle func(context.Context, *pubsub.Message)) {
	er1 := c.Subscription.Receive(ctx, func(ctx2 context.Context, msg *pubsub.Message) {
		if c.AckOnConsume {
			msg.Ack()
		}
		if len(c.ID) > 0 && len(msg.ID) > 0 {
			ctx2 = context.WithValue(ctx2, c.ID, msg.ID)
		}
		handle(ctx2, msg)
	})
	if er1 != nil {
		c.LogError(ctx, "Error when subscribe: "+er1.Error())
	}
}
func (c *Subscriber) SubscribeData(ctx context.Context, handle func(context.Context, []byte)) {
	er1 := c.Subscription.Receive(ctx, func(ctx2 context.Context, msg *pubsub.Message) {
		if msg != nil {
			if c.AckOnConsume {
				msg.Ack()
			}
			if len(c.ID) > 0 && len(msg.ID) > 0 {
				ctx2 = context.WithValue(ctx2, c.ID, msg.ID)
			}
			handle(ctx2, msg.Data)
		}
	})
	if er1 != nil {
		c.LogError(ctx, "Error when subscribe: "+er1.Error())
	}
}
func (c *Subscriber) Subscribe(ctx context.Context, handle func(context.Context, []byte, map[string]string)) {
	er1 := c.Subscription.Receive(ctx, func(ctx2 context.Context, msg *pubsub.Message) {
		if msg != nil {
			if c.AckOnConsume {
				msg.Ack()
			}
			if len(c.ID) > 0 && len(msg.ID) > 0 {
				ctx2 = context.WithValue(ctx2, c.ID, msg.ID)
			}
			handle(ctx2, msg.Data, msg.Attributes)
		}
	})
	if er1 != nil {
		c.LogError(ctx, "Error when subscribe: "+er1.Error())
	}
}
