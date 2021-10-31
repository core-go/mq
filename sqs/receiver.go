package sqs

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/core-go/mq"
)

type Receiver struct {
	Client            *sqs.SQS
	QueueURL          *string
	AckOnConsume      bool
	VisibilityTimeout int64 // should be 20 (seconds)
	WaitTimeSeconds   int64 // should be 0
	Convert           func(context.Context, []byte) ([]byte, error)
}

func NewReceiverByQueueName(client *sqs.SQS, queueName string, ackOnConsume bool, visibilityTimeout int64, waitTimeSeconds int64, options...func(context.Context, []byte)([]byte, error)) (*Receiver, error) {
	queueUrl, err := GetQueueUrl(client, queueName)
	if err != nil {
		return nil, err
	}
	return NewReceiver(client, queueUrl, ackOnConsume, visibilityTimeout, waitTimeSeconds, options...), nil
}

func NewReceiver(client *sqs.SQS, queueURL string, ackOnConsume bool, visibilityTimeout int64, waitTimeSeconds int64, options...func(context.Context, []byte)([]byte, error)) *Receiver {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Receiver{Client: client, QueueURL: &queueURL, AckOnConsume: ackOnConsume, VisibilityTimeout: visibilityTimeout, WaitTimeSeconds: waitTimeSeconds, Convert: convert}
}

func (c *Receiver) Receive(ctx context.Context, handle func(context.Context, *mq.Message, error) error) {
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
		handle(ctx, nil, er1)
	} else {
		if len(result.Messages) > 0 {
			m := result.Messages[0]
			data := []byte(*m.Body)
			attributes := PtrToMap(m.Attributes)
			message := mq.Message{
				Id:         *m.MessageId,
				Data:       data,
				Attributes: attributes,
				Raw:        m,
			}
			if c.AckOnConsume {
				_, er2 := c.Client.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      c.QueueURL,
					ReceiptHandle: result.Messages[0].ReceiptHandle,
				})
				if er2 != nil {
					handle(ctx, nil, er2)
				} else {
					if c.Convert == nil {
						handle(ctx, &message, nil)
					} else {
						data0, err := c.Convert(ctx, data)
						if err == nil {
							message.Data = data0
						}
						handle(ctx, &message, err)
					}
				}
			} else {
				if c.Convert == nil {
					handle(ctx, &message, nil)
				} else {
					data0, err := c.Convert(ctx, data)
					if err == nil {
						message.Data = data0
					}
					handle(ctx, &message, err)
				}
			}
		}
	}
	goto loop
}
