package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/core-go/mq"
)

type Subscriber struct {
	Client       *pubsub.Client
	Subscription *pubsub.Subscription
	AckOnConsume bool
}

func NewSubscriber(client *pubsub.Client, subscriptionId string, c SubscriptionConfig, ackOnConsume bool) *Subscriber {
	subscription := client.Subscription(subscriptionId)
	return &Subscriber{Client: client, Subscription: ConfigureSubscription(subscription, c), AckOnConsume: ackOnConsume}
}

func NewSubscriberByConfig(ctx context.Context, c SubscriberConfig, ackOnConsume bool) (*Subscriber, error) {
	if c.Retry.Retry1 <= 0 {
		client, err := NewPubSubClient(ctx, []byte(c.Client.Credentials), c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewSubscriber(client, c.SubscriptionId, c.SubscriptionConfig, ackOnConsume), nil
	} else {
		durations := DurationsFromValue(c.Retry, "Retry", 9)
		client, err := NewPubSubClientWithRetries(ctx, []byte(c.Client.Credentials), durations, c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewSubscriber(client, c.SubscriptionId, c.SubscriptionConfig, ackOnConsume), nil
	}
}

func ConfigureSubscription(subscription *pubsub.Subscription, c SubscriptionConfig) *pubsub.Subscription {
	if c.MaxOutstandingMessages > 0 {
		subscription.ReceiveSettings.MaxOutstandingMessages = c.MaxOutstandingMessages
	}
	if c.NumGoroutines > 0 {
		subscription.ReceiveSettings.NumGoroutines = c.NumGoroutines
	}
	return subscription
}

func (c *Subscriber) Subscribe(ctx context.Context, handle func(context.Context, *mq.Message, error) error) {
	er1 := c.Subscription.Receive(ctx, func(ctx2 context.Context, m *pubsub.Message) {
		message := mq.Message{
			Id:         m.ID,
			Data:       m.Data,
			Attributes: m.Attributes,
			Raw:        m,
		}
		if c.AckOnConsume {
			m.Ack()
		}
		handle(ctx2, &message, nil)
	})
	if er1 != nil {
		handle(ctx, nil, er1)
	}
}
