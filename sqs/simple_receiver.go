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
	Convert           func(context.Context, []byte) ([]byte, error)
}

func NewSimpleReceiverByQueueName(client *sqs.SQS, queueName string, ackOnConsume bool, visibilityTimeout int64, waitTimeSeconds int64, options...func(context.Context, []byte)([]byte, error)) (*SimpleReceiver, error) {
	queueUrl, err := GetQueueUrl(client, queueName)
	if err != nil {
		return nil, err
	}
	return NewSimpleReceiver(client, queueUrl, ackOnConsume, visibilityTimeout, waitTimeSeconds, options...), nil
}

func NewSimpleReceiver(client *sqs.SQS, queueURL string, ackOnConsume bool, visibilityTimeout int64, waitTimeSeconds int64, options...func(context.Context, []byte)([]byte, error)) *SimpleReceiver {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &SimpleReceiver{Client: client, QueueURL: &queueURL, AckOnConsume: ackOnConsume, VisibilityTimeout: visibilityTimeout, WaitTimeSeconds: waitTimeSeconds, Convert: convert}
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
					if c.Convert == nil {
						handle(ctx, data, attributes, nil)
					} else {
						data0, err := c.Convert(ctx, data)
						handle(ctx, data0, attributes, err)
					}
				}
			} else {
				if c.Convert == nil {
					handle(ctx, data, attributes, nil)
				} else {
					data0, err := c.Convert(ctx, data)
					handle(ctx, data0, attributes, err)
				}
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
