package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func GetQueueUrl(client *sqs.SQS, queueName string) (string, error) {
	result, err := client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})
	if err != nil {
		return "", err
	}
	return *result.QueueUrl, err
}

func MapToAttributes(attributes map[string]string) map[string]*sqs.MessageAttributeValue {
	attrs := make(map[string]*sqs.MessageAttributeValue)
	if attributes != nil {
		for k, v := range attributes {
			x := sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(v),
			}
			attrs[k] = &x
		}
	}
	return attrs
}
