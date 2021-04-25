package mq

import "context"

type BatchWorker interface {
	Handle(ctx context.Context, message *Message)
	Run(ctx context.Context)
}
