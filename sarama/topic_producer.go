package kafka

import (
	"context"
	"github.com/IBM/sarama"
)

type TopicProducer struct {
	SyncProducer sarama.SyncProducer
}

func NewTopicProducer(writer sarama.SyncProducer) (*TopicProducer, error) {
	return &TopicProducer{SyncProducer: writer}, nil
}
func NewTopicProducerByConfig(c ProducerConfig) (*TopicProducer, error) {
	writer, err := newSyncProducer(c)
	if err != nil {
		return nil, err
	}
	return NewTopicProducer(*writer)
}
func (p *TopicProducer) Produce(ctx context.Context, topic string, data []byte, messageAttributes map[string]string) error {
	msg := sarama.ProducerMessage{Value: sarama.ByteEncoder(data), Topic: topic}
	if messageAttributes != nil {
		msg.Headers = MapToHeader(messageAttributes)
	}
	_, _, err := p.SyncProducer.SendMessage(&msg)
	return err
}
func (p *TopicProducer) ProduceValue(ctx context.Context, topic string, data []byte) error {
	msg := sarama.ProducerMessage{Value: sarama.ByteEncoder(data), Topic: topic}
	_, _, err := p.SyncProducer.SendMessage(&msg)
	return err
}
func (p *TopicProducer) ProduceWithKey(topic string, data []byte, key string, messageAttributes map[string]string) error {
	msg := sarama.ProducerMessage{Value: sarama.ByteEncoder(data), Topic: topic}
	if messageAttributes != nil {
		msg.Headers = MapToHeader(messageAttributes)
	}
	if len(key) > 0 {
		msg.Key = sarama.StringEncoder(key)
	}
	_, _, err := p.SyncProducer.SendMessage(&msg)
	return err
}
func (p *TopicProducer) ProduceMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	return p.SyncProducer.SendMessage(msg)
}
