package mq

import "context"

type Consumer interface {
	Consume(ctx context.Context, handle func(context.Context, *Message, error) error)
}
