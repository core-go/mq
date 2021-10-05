package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
)

type SimplePublisher struct {
	Client *pubsub.Client
	Config *TopicConfig
}

func NewSimplePublisher(ctx context.Context, client *pubsub.Client, c *TopicConfig) *SimplePublisher {
	return &SimplePublisher{Client: client, Config: c}
}

func NewSimplePublisherByConfig(ctx context.Context, c PublisherConfig) (*SimplePublisher, error) {
	if c.Retry.Retry1 <= 0 {
		client, err := NewPubSubClient(ctx, []byte(c.Client.Credentials), c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewSimplePublisher(ctx, client, c.Topic), nil
	} else {
		durations := DurationsFromValue(c.Retry, "Retry", 9)
		client, err := NewPubSubClientWithRetries(ctx, []byte(c.Client.Credentials), durations, c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewSimplePublisher(ctx, client, c.Topic), nil
	}
}

func (c *SimplePublisher) Publish(ctx context.Context, topicId string, data []byte, attributes map[string]string) (string, error) {
	msg := &pubsub.Message{
		Data: data,
	}

	if attributes != nil {
		msg.Attributes = attributes
	}
	topic := c.Client.Topic(topicId)
	topic = ConfigureTopic(topic, c.Config)
	publishResult := topic.Publish(ctx, msg)
	return publishResult.Get(ctx)
}
