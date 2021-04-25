package sqs

import (
	"context"
	"github.com/aws/aws-sdk-go/service/sqs"
	"time"
)

type HealthChecker struct {
	Client    *sqs.SQS
	QueueName *string
	Service   string
	Timeout   time.Duration
}

func NewHealthChecker(client *sqs.SQS, queueName string, options ...string) *HealthChecker {
	var name string
	if len(options) > 0 && len(options[0]) > 0 {
		name = options[0]
	} else {
		name = "sqs"
	}
	return NewSQSHealthChecker(client, name, queueName)
}
func NewSQSHealthChecker(client *sqs.SQS, name string, queueName string, options ...time.Duration) *HealthChecker {
	var timeout time.Duration
	if len(options) >= 1 && options[0] > 0 {
		timeout = options[0]
	} else {
		timeout = 4 * time.Second
	}
	return &HealthChecker{Client: client, QueueName: &queueName, Service: name, Timeout: timeout}
}

func (h *HealthChecker) Name() string {
	return h.Service
}

func (h *HealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	_, err := h.Client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: h.QueueName,
	})
	return res, err
}

func (h *HealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	data["error"] = err.Error()
	return data
}
