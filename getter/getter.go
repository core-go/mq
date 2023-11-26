package getter

import "context"

type Getter struct {
	Get func(ctx context.Context, handle func(ctx context.Context, data []byte, header map[string]string, err error) error)
	Handle func(ctx context.Context, data []byte, header map[string]string, err error) error
}

type Handle func(ctx context.Context, data []byte, header map[string]string, err error) error
type Get func(ctx context.Context, handle func(ctx context.Context, data []byte, header map[string]string, err error) error)
