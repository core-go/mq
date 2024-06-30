package sqs

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type QueueSender struct {
	Client       *sqs.SQS
	DelaySeconds *int64 //could be 10
}

func NewQueueSender(client *sqs.SQS, delaySeconds int64) *QueueSender {
	return &QueueSender{Client: client, DelaySeconds: &delaySeconds}
}
func (p *QueueSender) SendMessage(ctx context.Context, queueName string, data []byte, attributes map[string]string) (string, error) {
	queueUrl, er0 := GetQueueUrl(p.Client, queueName)
	if er0 != nil {
		return "", er0
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
func (p *QueueSender) Send(ctx context.Context, queueName string, data []byte, attributes map[string]string) error {
	queueUrl, er0 := GetQueueUrl(p.Client, queueName)
	if er0 != nil {
		return er0
	}
	attrs := MapToAttributes(attributes)
	s := string(data)
	_, err := p.Client.SendMessage(&sqs.SendMessageInput{
		DelaySeconds:      p.DelaySeconds,
		MessageAttributes: attrs,
		MessageBody:       aws.String(s),
		QueueUrl:          &queueUrl,
	})
	return err
}
func (p *QueueSender) SendBody(ctx context.Context, queueName string, data []byte) error {
	queueUrl, er0 := GetQueueUrl(p.Client, queueName)
	if er0 != nil {
		return er0
	}
	s := string(data)
	_, err := p.Client.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: p.DelaySeconds,
		MessageBody:  aws.String(s),
		QueueUrl:     &queueUrl,
	})
	return err
}
