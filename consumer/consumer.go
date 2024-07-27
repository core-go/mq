package consumer

import "context"

type SimpleConsumer struct {
	Consume func(ctx context.Context, handle func(context.Context, []byte)
	Handle  func(context.Context, []byte)
}

type Consumer struct {
	Consume func(ctx context.Context, handle func(context.Context, []byte, map[string]string))
	Handle  func(context.Context, []byte, map[string]string)
}

type Handle func(context.Context, []byte, map[string]string)
type Consume func(context.Context, func(context.Context, []byte, map[string]string))
