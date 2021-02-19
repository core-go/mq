package mq

import "context"

type BatchWorker interface {
	Consume(ctx context.Context, message *Message)
	Run(ctx context.Context)
}
