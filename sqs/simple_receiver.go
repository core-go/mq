package sqs

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SimpleReceiver struct {
	Client            *sqs.SQS
	QueueURL          *string
	AckOnConsume      bool
	VisibilityTimeout int64 // should be 20 (seconds)
	WaitTimeSeconds   int64 // should be 0
}

func NewSimpleReceiverByQueueName(client *sqs.SQS, queueName string, ackOnConsume bool, visibilityTimeout int64, waitTimeSeconds int64) (*SimpleReceiver, error) {
	queueUrl, err := GetQueueUrl(client, queueName)
	if err != nil {
		return nil, err
	}
	return NewSimpleReceiver(client, queueUrl, ackOnConsume, visibilityTimeout, waitTimeSeconds), nil
}

func NewSimpleReceiver(client *sqs.SQS, queueURL string, ackOnConsume bool, visibilityTimeout int64, waitTimeSeconds int64) *SimpleReceiver {
	return &SimpleReceiver{Client: client, QueueURL: &queueURL, AckOnConsume: ackOnConsume, VisibilityTimeout: visibilityTimeout, WaitTimeSeconds: waitTimeSeconds}
}

func (c *SimpleReceiver) Receive(ctx context.Context, handle func(context.Context, []byte, map[string]string, error) error) {
	var result *sqs.ReceiveMessageOutput
	var er1 error
loop:
	result, er1 = c.Client.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            c.QueueURL,
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   aws.Int64(c.VisibilityTimeout), // 20 seconds
		WaitTimeSeconds:     aws.Int64(c.WaitTimeSeconds),
	})
	if er1 != nil {
		handle(ctx, nil, nil, er1)
	} else {
		if len(result.Messages) > 0 {
			m := result.Messages[0]
			data := []byte(*m.Body)
			attributes := PtrToMap(m.Attributes)
			if c.AckOnConsume {
				_, er2 := c.Client.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      c.QueueURL,
					ReceiptHandle: result.Messages[0].ReceiptHandle,
				})
				if er2 != nil {
					handle(ctx, data, attributes, er2)
				} else {
					handle(ctx, data, attributes, nil)
				}
			} else {
				handle(ctx, data, attributes, nil)
			}
		}
	}
	goto loop
}

func PtrToMap(m map[string]*string) map[string]string {
	attributes := make(map[string]string)
	for k, v := range m {
		if v != nil {
			attributes[k] = *v
		}
	}
	return attributes
}
