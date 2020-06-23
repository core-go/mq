package mq

import "context"

type ConsumerCaller interface {
	Call(ctx context.Context, msg *Message, err error) error
}
