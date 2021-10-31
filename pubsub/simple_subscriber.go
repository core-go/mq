package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
)

type SimpleSubscriber struct {
	Client       *pubsub.Client
	Subscription *pubsub.Subscription
	AckOnConsume bool
	Convert      func(context.Context, []byte) ([]byte, error)
}

func NewSimpleSubscriber(client *pubsub.Client, subscriptionId string, c SubscriptionConfig, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) *SimpleSubscriber {
	subscription := client.Subscription(subscriptionId)
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &SimpleSubscriber{Client: client, Subscription: ConfigureSubscription(subscription, c), AckOnConsume: ackOnConsume, Convert: convert}
}

func NewSimpleSubscriberByConfig(ctx context.Context, c SubscriberConfig, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) (*SimpleSubscriber, error) {
	if c.Retry.Retry1 <= 0 {
		client, err := NewPubSubClient(ctx, []byte(c.Client.Credentials), c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewSimpleSubscriber(client, c.SubscriptionId, c.SubscriptionConfig, ackOnConsume, options...), nil
	} else {
		durations := DurationsFromValue(c.Retry, "Retry", 9)
		client, err := NewPubSubClientWithRetries(ctx, []byte(c.Client.Credentials), durations, c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewSimpleSubscriber(client, c.SubscriptionId, c.SubscriptionConfig, ackOnConsume, options...), nil
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
		if c.Convert == nil {
			handle(ctx2, m.Data, m.Attributes, nil)
		} else {
			data, err := c.Convert(ctx, m.Data)
			handle(ctx2, data, m.Attributes, err)
		}
	})
	if er1 != nil {
		handle(ctx, nil, nil, er1)
	}
}
