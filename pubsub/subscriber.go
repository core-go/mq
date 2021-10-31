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
	Convert      func(context.Context, []byte) ([]byte, error)
}

func NewSubscriber(client *pubsub.Client, subscriptionId string, c SubscriptionConfig, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) *Subscriber {
	subscription := client.Subscription(subscriptionId)
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Subscriber{Client: client, Subscription: ConfigureSubscription(subscription, c), AckOnConsume: ackOnConsume, Convert: convert}
}

func NewSubscriberByConfig(ctx context.Context, c SubscriberConfig, ackOnConsume bool, options...func(context.Context, []byte)([]byte, error)) (*Subscriber, error) {
	if c.Retry.Retry1 <= 0 {
		client, err := NewPubSubClient(ctx, []byte(c.Client.Credentials), c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewSubscriber(client, c.SubscriptionId, c.SubscriptionConfig, ackOnConsume, options...), nil
	} else {
		durations := DurationsFromValue(c.Retry, "Retry", 9)
		client, err := NewPubSubClientWithRetries(ctx, []byte(c.Client.Credentials), durations, c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewSubscriber(client, c.SubscriptionId, c.SubscriptionConfig, ackOnConsume, options...), nil
	}
}

func (c *Subscriber) Subscribe(ctx context.Context, handle func(context.Context, *mq.Message, error) error) {
	er1 := c.Subscription.Receive(ctx, func(ctx2 context.Context, m *pubsub.Message) {
		message := mq.Message{
			Id:         m.ID,
			Data:       m.Data,
			Attributes: m.Attributes,
			Timestamp:  &m.PublishTime,
			Raw:        m,
		}
		if c.AckOnConsume {
			m.Ack()
		}
		if c.Convert == nil {
			handle(ctx2, &message, nil)
		} else {
			data, err := c.Convert(ctx, m.Data)
			if err == nil {
				message.Data = data
			}
			handle(ctx2, &message, nil)
		}
	})
	if er1 != nil {
		handle(ctx, nil, er1)
	}
}
