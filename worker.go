package mq

import "context"

type Worker interface {
	OnConsume(ctx context.Context, message *Message)
}
