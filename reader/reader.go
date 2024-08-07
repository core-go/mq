package reader

import "context"

type SimpleReader struct {
	Read   func(ctx context.Context, handle func(context.Context, []byte)
	Handle func(context.Context, []byte)
}

type Reader struct {
	Read   func(ctx context.Context, handle func(context.Context, []byte, map[string]string))
	Handle func(context.Context, []byte, map[string]string)
}

type Handle func(context.Context, []byte, map[string]string)
type Read func(context.Context, func(context.Context, []byte, map[string]string))
