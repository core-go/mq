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
}

func NewSenderByQueueName(client *sqs.SQS, queueName string, options... int64) (*Sender, error) {
	queueUrl, err := GetQueueUrl(client, queueName)
	if err != nil {
		return nil, err
	}
	return NewSender(client, queueUrl, options...), nil
}

func NewSender(client *sqs.SQS, queueURL string, options... int64) *Sender {
	var delaySeconds int64
	if len(options) > 0 && options[0] >= 0 {
		delaySeconds = options[0]
	} else {
		delaySeconds = 10
	}
	return &Sender{Client: client, QueueURL: &queueURL, DelaySeconds: &delaySeconds}
}

func (p *Sender) Send(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	attrs := MapToAttributes(attributes)
	s := string(data)
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
