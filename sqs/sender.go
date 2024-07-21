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

func NewSenderByQueueName(client *sqs.SQS, queueName string, delaySeconds int64) (*Sender, error) {
	queueUrl, err := GetQueueUrl(client, queueName)
	if err != nil {
		return nil, err
	}
	return NewSender(client, queueUrl, delaySeconds), nil
}

func NewSender(client *sqs.SQS, queueURL string, delaySeconds int64) *Sender {
	return &Sender{Client: client, QueueURL: &queueURL, DelaySeconds: &delaySeconds}
}
func (p *Sender) Send(ctx context.Context, data []byte, attributes map[string]string) error {
	attrs := MapToAttributes(attributes)
	s := string(data)
	_, err := p.Client.SendMessage(&sqs.SendMessageInput{
		DelaySeconds:      p.DelaySeconds,
		MessageAttributes: attrs,
		MessageBody:       aws.String(s),
		QueueUrl:          p.QueueURL,
	})
	return err
}
func (p *Sender) SendBody(ctx context.Context, data []byte) error {
	s := string(data)
	_, err := p.Client.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: p.DelaySeconds,
		MessageBody:  aws.String(s),
		QueueUrl:     p.QueueURL,
	})
	return err
}
func (p *Sender) SendMessage(msg *sqs.SendMessageInput) (string, error) {
	if msg == nil {
		return "", nil
	}
	result, err := p.Client.SendMessage(msg)
	if result != nil && result.MessageId != nil {
		return *result.MessageId, err
	} else {
		return "", err
	}
}
