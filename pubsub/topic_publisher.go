package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
)

type TopicPublisher struct {
	Client *pubsub.Client
	Config *TopicConfig
}

func NewTopicPublisher(client *pubsub.Client, c *TopicConfig) *TopicPublisher {
	return &TopicPublisher{Client: client, Config: c}
}

func NewTopicPublisherByConfig(ctx context.Context, c PublisherConfig) (*TopicPublisher, error) {
	if c.Retry.Retry1 <= 0 {
		client, err := NewPubSubClient(ctx, []byte(c.Client.Credentials), c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewTopicPublisher(client, c.Topic), nil
	} else {
		durations := DurationsFromValue(c.Retry, "Retry", 9)
		client, err := NewPubSubClientWithRetries(ctx, []byte(c.Client.Credentials), durations, c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewTopicPublisher(client, c.Topic), nil
	}
}
func (p *TopicPublisher) Publish(ctx context.Context, topicId string, data []byte, attributes map[string]string) error {
	msg := &pubsub.Message{Data: data}
	if attributes != nil {
		msg.Attributes = attributes
	}
	topic := p.Client.Topic(topicId)
	topic = ConfigureTopic(topic, p.Config)
	publishResult := topic.Publish(ctx, msg)
	_, err := publishResult.Get(ctx)
	return err
}
func (p *TopicPublisher) PublishData(ctx context.Context, topicId string, data []byte) error {
	msg := &pubsub.Message{Data: data}
	topic := p.Client.Topic(topicId)
	topic = ConfigureTopic(topic, p.Config)
	publishResult := topic.Publish(ctx, msg)
	_, err := publishResult.Get(ctx)
	return err
}
func (p *TopicPublisher) PublishMessage(ctx context.Context, topicId string, data []byte, attributes map[string]string) (string, error) {
	msg := &pubsub.Message{Data: data}
	if attributes != nil {
		msg.Attributes = attributes
	}
	topic := p.Client.Topic(topicId)
	topic = ConfigureTopic(topic, p.Config)
	publishResult := topic.Publish(ctx, msg)
	return publishResult.Get(ctx)
}
