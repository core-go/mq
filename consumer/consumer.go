package consumer

import "context"

type Consumer struct {
	Consume func(ctx context.Context, handle func(ctx context.Context, data []byte, header map[string]string, err error) error)
	Handle func(ctx context.Context, data []byte, header map[string]string, err error) error
}

type Handle func(ctx context.Context, data []byte, header map[string]string, err error) error
type Consume func(ctx context.Context, handle func(ctx context.Context, data []byte, header map[string]string, err error) error)
