package reader

import "context"

type Reader interface {
	Read(ctx context.Context, handle func(ctx context.Context, data []byte, header map[string]string, err error) error)
	Handle(ctx context.Context, data []byte, header map[string]string, err error) error
}
type Handle func(ctx context.Context, data []byte, header map[string]string, err error) error
type Read func(ctx context.Context, handle func(ctx context.Context, data []byte, header map[string]string, err error) error)
