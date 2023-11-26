package subscriber

import "context"

type Subscriber interface {
	Subscribe(ctx context.Context, handle func(ctx context.Context, data []byte, header map[string]string, err error) error)
	Handle(ctx context.Context, data []byte, header map[string]string, err error) error
}

type Handle func(ctx context.Context, data []byte, header map[string]string, err error) error
type Subscribe func(ctx context.Context, handle func(ctx context.Context, data []byte, header map[string]string, err error) error)
