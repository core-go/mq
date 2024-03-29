package receiver

import "context"

type Receiver struct {
	Receive func(ctx context.Context, handle func(ctx context.Context, data []byte, header map[string]string, err error) error)
	Handle func(ctx context.Context, data []byte, header map[string]string, err error) error
}

type Handle func(ctx context.Context, data []byte, header map[string]string, err error) error
type Receive func(ctx context.Context, handle func(ctx context.Context, data []byte, header map[string]string, err error) error)
