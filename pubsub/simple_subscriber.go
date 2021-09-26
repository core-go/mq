package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
)

type SimpleSubscriber struct {
	Client       *pubsub.Client
	Subscription *pubsub.Subscription
	AckOnConsume bool
}

func NewSimpleSubscriber(client *pubsub.Client, subscriptionId string, c SubscriptionConfig, ackOnConsume bool) *SimpleSubscriber {
	subscription := client.Subscription(subscriptionId)
	return &SimpleSubscriber{Client: client, Subscription: ConfigureSubscription(subscription, c), AckOnConsume: ackOnConsume}
}

func NewSimpleSubscriberByConfig(ctx context.Context, c SubscriberConfig, ackOnConsume bool) (*SimpleSubscriber, error) {
	if c.Retry.Retry1 <= 0 {
		client, err := NewPubSubClient(ctx, []byte(c.Client.Credentials), c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewSimpleSubscriber(client, c.SubscriptionId, c.SubscriptionConfig, ackOnConsume), nil
	} else {
		durations := DurationsFromValue(c.Retry, "Retry", 9)
		client, err := NewPubSubClientWithRetries(ctx, []byte(c.Client.Credentials), durations, c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewSimpleSubscriber(client, c.SubscriptionId, c.SubscriptionConfig, ackOnConsume), nil
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

func (c *SimpleSubscriber) Subscribe(ctx context.Context, handle func(context.Context, []byte, map[string]string, error) error) {
	er1 := c.Subscription.Receive(ctx, func(ctx2 context.Context, m *pubsub.Message) {
		if c.AckOnConsume {
			m.Ack()
		}
		handle(ctx2, m.Data, m.Attributes, nil)
	})
	if er1 != nil {
		handle(ctx, nil, nil, er1)
	}
}
