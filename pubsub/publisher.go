package pubsub

import (
	"cloud.google.com/go/iam"
	"cloud.google.com/go/pubsub"
	"context"
	"log"
	"time"
)

var CheckTopicPermission = CheckPermission

type Publisher struct {
	Client *pubsub.Client
	Topic  *pubsub.Topic
}

func NewPublisher(ctx context.Context, client *pubsub.Client, topicId string, c TopicConfig) *Publisher {
	topic := client.Topic(topicId)
	CheckTopicPermission(ctx, topic.IAM(), "pubsub.topics.publish")
	return &Publisher{Client: client, Topic: ConfigureTopic(topic, c)}
}

func NewPublisherByConfig(ctx context.Context, c PublisherConfig) (*Publisher, error) {
	if c.Retry.Retry1 <= 0 {
		client, err := NewPubSubClient(ctx, c.Client.ProjectId, c.Client.Credentials)
		if err != nil {
			return nil, err
		}
		return NewPublisher(ctx, client, c.TopicId, c.Topic), nil
	} else {
		durations := DurationsFromValue(c.Retry, "Retry", 9)
		client, err := NewPubSubClientWithRetries(ctx, c.Client.ProjectId, c.Client.Credentials, durations)
		if err != nil {
			return nil, err
		}
		return NewPublisher(ctx, client, c.TopicId, c.Topic), nil
	}
}

func ConfigureTopic(topic *pubsub.Topic, c TopicConfig) *pubsub.Topic {
	if c.CountThreshold > 0 {
		topic.PublishSettings.DelayThreshold = time.Duration(c.CountThreshold) * time.Millisecond
	}
	if c.DelayThreshold > 0 {
		topic.PublishSettings.CountThreshold = c.DelayThreshold
	}
	if c.ByteThreshold > 0 {
		topic.PublishSettings.ByteThreshold = c.ByteThreshold
	}
	if c.NumGoroutines > 0 {
		topic.PublishSettings.NumGoroutines = c.NumGoroutines
	}
	return topic
}

func (c *Publisher) Publish(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	msg := &pubsub.Message{
		Data: data,
	}

	if attributes != nil {
		msg.Attributes = attributes
	}

	publishResult := c.Topic.Publish(ctx, msg)
	return publishResult.Get(ctx)
}

func CheckPermission(ctx0 context.Context, iam *iam.Handle, permission string) {
	ctx, _ := context.WithTimeout(ctx0, 30*time.Second)

	log.Printf("Checking permission: %s", permission)
	if permissions, err := iam.TestPermissions(ctx, []string{permission}); err != nil {
		log.Printf("Can't check permission %v: %s", permission, err.Error())
	} else if len(permissions) > 0 && permissions[0] == permission {
		log.Printf("Permission %v valid", permission)
	} else {
		log.Printf("Permission %v invalid", permission)
	}
}
