package sqs

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SimpleSender struct {
	Client       *sqs.SQS
	DelaySeconds *int64 //could be 10
}

func NewSimpleSender(client *sqs.SQS, options... int64) *SimpleSender {
	var delaySeconds int64
	if len(options) > 0 && options[0] >= 0 {
		delaySeconds = options[0]
	} else {
		delaySeconds = 10
	}
	return &SimpleSender{Client: client, DelaySeconds: &delaySeconds}
}


func (p *SimpleSender) Send(ctx context.Context, queueName string, data []byte, attributes map[string]string) (string, error) {
	queueUrl, err := GetQueueUrl(p.Client, queueName)
	if err != nil {
		return "", err
	}
	attrs := MapToAttributes(attributes)
	s := string(data)
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
