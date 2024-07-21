package getter

import "context"

type Getter struct {
	Get    func(ctx context.Context, handle func(context.Context, []byte, map[string]string))
	Handle func(context.Context, []byte, map[string]string)
}

type Handle func(context.Context, []byte, map[string]string)
type Get func(context.Context, func(context.Context, []byte, map[string]string))
