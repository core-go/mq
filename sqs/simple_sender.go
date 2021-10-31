package sqs

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SimpleSender struct {
	Client       *sqs.SQS
	DelaySeconds *int64 //could be 10
	Convert      func(context.Context, []byte) ([]byte, error)
}

func NewSimpleSender(client *sqs.SQS, delaySeconds int64, options ...func(context.Context, []byte) ([]byte, error)) *SimpleSender {
	var convert func(context.Context, []byte) ([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &SimpleSender{Client: client, DelaySeconds: &delaySeconds, Convert: convert}
}

func (p *SimpleSender) Send(ctx context.Context, queueName string, data []byte, attributes map[string]string) (string, error) {
	queueUrl, err := GetQueueUrl(p.Client, queueName)
	if err != nil {
		return "", err
	}
	attrs := MapToAttributes(attributes)
	var binary = data
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
		QueueUrl:          &queueUrl,
	})
	if result != nil && result.MessageId != nil {
		return *result.MessageId, err
	} else {
		return "", err
	}
}
