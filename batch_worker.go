package mq

import "context"

type BatchWorker interface {
	OnConsume(ctx context.Context, message *Message)
	RunScheduler(ctx context.Context)
}
