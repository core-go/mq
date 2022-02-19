package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
)

type SimplePublisher struct {
	Client *pubsub.Client
	Config *TopicConfig
	Convert func(context.Context, []byte)([]byte, error)
}

func NewSimplePublisher(client *pubsub.Client, c *TopicConfig, options...func(context.Context, []byte)([]byte, error)) *SimplePublisher {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &SimplePublisher{Client: client, Config: c, Convert: convert}
}

func NewSimplePublisherByConfig(ctx context.Context, c PublisherConfig, options...func(context.Context, []byte)([]byte, error)) (*SimplePublisher, error) {
	if c.Retry.Retry1 <= 0 {
		client, err := NewPubSubClient(ctx, []byte(c.Client.Credentials), c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewSimplePublisher(client, c.Topic, options...), nil
	} else {
		durations := DurationsFromValue(c.Retry, "Retry", 9)
		client, err := NewPubSubClientWithRetries(ctx, []byte(c.Client.Credentials), durations, c.Client.ProjectId)
		if err != nil {
			return nil, err
		}
		return NewSimplePublisher(client, c.Topic, options...), nil
	}
}
func (p *SimplePublisher) Put(ctx context.Context, topicId string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, topicId, data, attributes)
}
func (p *SimplePublisher) Send(ctx context.Context, topicId string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, topicId, data, attributes)
}
func (p *SimplePublisher) Write(ctx context.Context, topicId string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, topicId, data, attributes)
}
func (p *SimplePublisher) Produce(ctx context.Context, topicId string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, topicId, data, attributes)
}
func (p *SimplePublisher) Publish(ctx context.Context, topicId string, data []byte, attributes map[string]string) (string, error) {
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, data)
		if err != nil {
			return "", err
		}
	}
	msg := &pubsub.Message{
		Data: binary,
	}
	if attributes != nil {
		msg.Attributes = attributes
	}
	topic := p.Client.Topic(topicId)
	topic = ConfigureTopic(topic, p.Config)
	publishResult := topic.Publish(ctx, msg)
	return publishResult.Get(ctx)
}
