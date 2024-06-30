package sqs

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Receiver struct {
	Client            *sqs.SQS
	QueueURL          *string
	AckOnConsume      bool
	VisibilityTimeout int64 // should be 20 (seconds)
	WaitTimeSeconds   int64 // should be 0
	LogError          func(ctx context.Context, msg string)
}

func NewReceiverByQueueName(client *sqs.SQS, queueName string, ackOnConsume bool, visibilityTimeout int64, waitTimeSeconds int64) (*Receiver, error) {
	queueUrl, err := GetQueueUrl(client, queueName)
	if err != nil {
		return nil, err
	}
	return NewReceiver(client, queueUrl, ackOnConsume, visibilityTimeout, waitTimeSeconds), nil
}

func NewReceiver(client *sqs.SQS, queueURL string, ackOnConsume bool, visibilityTimeout int64, waitTimeSeconds int64) *Receiver {
	return &Receiver{Client: client, QueueURL: &queueURL, AckOnConsume: ackOnConsume, VisibilityTimeout: visibilityTimeout, WaitTimeSeconds: waitTimeSeconds}
}

func (c *Receiver) Receive(ctx context.Context, handle func(context.Context, []byte, map[string]string)) {
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
		c.LogError(ctx, "Error when subscribe: "+er1.Error())
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
					c.LogError(ctx, "Error when delete message: "+er2.Error())
				} else {
					handle(ctx, data, attributes)
				}
			} else {
				handle(ctx, data, attributes)
			}
		}
	}
	goto loop
}
func (c *Receiver) ReceiveBody(ctx context.Context, handle func(context.Context, []byte)) {
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
		c.LogError(ctx, "Error when subscribe: "+er1.Error())
	} else {
		if len(result.Messages) > 0 {
			m := result.Messages[0]
			data := []byte(*m.Body)
			if c.AckOnConsume {
				_, er2 := c.Client.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      c.QueueURL,
					ReceiptHandle: result.Messages[0].ReceiptHandle,
				})
				if er2 != nil {
					c.LogError(ctx, "Error when delete message: "+er2.Error())
				} else {
					handle(ctx, data)
				}
			} else {
				handle(ctx, data)
			}
		}
	}
	goto loop
}
func (c *Receiver) ReceiveMessage(ctx context.Context, handle func(context.Context, *sqs.Message)) {
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
		c.LogError(ctx, "Error when subscribe: "+er1.Error())
	} else {
		if len(result.Messages) > 0 {
			m := result.Messages[0]
			if c.AckOnConsume {
				_, er2 := c.Client.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      c.QueueURL,
					ReceiptHandle: result.Messages[0].ReceiptHandle,
				})
				if er2 != nil {
					c.LogError(ctx, "Error when delete message: "+er2.Error())
				} else {
					handle(ctx, m)
				}
			} else {
				handle(ctx, m)
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
