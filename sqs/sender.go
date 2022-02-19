package sqs

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Sender struct {
	Client       *sqs.SQS
	QueueURL     *string
	DelaySeconds *int64 //could be 10
	Convert      func(context.Context, []byte) ([]byte, error)
}

func NewSenderByQueueName(client *sqs.SQS, queueName string, delaySeconds int64, options...func(context.Context, []byte)([]byte, error)) (*Sender, error) {
	queueUrl, err := GetQueueUrl(client, queueName)
	if err != nil {
		return nil, err
	}
	return NewSender(client, queueUrl, delaySeconds, options...), nil
}

func NewSender(client *sqs.SQS, queueURL string, delaySeconds int64, options...func(context.Context, []byte)([]byte, error)) *Sender {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Sender{Client: client, QueueURL: &queueURL, DelaySeconds: &delaySeconds, Convert: convert}
}
func (p *Sender) Put(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Send(ctx, data, attributes)
}
func (p *Sender) Write(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Send(ctx, data, attributes)
}
func (p *Sender) Produce(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Send(ctx, data, attributes)
}
func (p *Sender) Publish(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Send(ctx, data, attributes)
}
func (p *Sender) Send(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	attrs := MapToAttributes(attributes)
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, data)
		if err != nil {
			return "", err
		}
	}
	s := string(binary)
	result, err := p.Client.SendMessage(&sqs.SendMessageInput{
		DelaySeconds:      p.DelaySeconds,
		MessageAttributes: attrs,
		MessageBody:       aws.String(s),
		QueueUrl:          p.QueueURL,
	})
	if result != nil && result.MessageId != nil {
		return *result.MessageId, err
	} else {
		return "", err
	}
}
