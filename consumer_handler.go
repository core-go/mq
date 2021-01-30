package mq

import "context"

type ConsumerHandler interface {
	Handle(ctx context.Context, msg *Message, err error) error
}
