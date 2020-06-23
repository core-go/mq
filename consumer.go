package mq

import "context"

type Consumer interface {
	Consume(ctx context.Context, caller ConsumerCaller)
}
