package mq

import "context"

type BatchHandler interface {
	Handle(ctx context.Context, data []*Message) ([]*Message, error)
}
