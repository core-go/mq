package subscriber

import "context"

type SimpleSubscriber struct {
	Subscribe func(ctx context.Context, handle func(context.Context, []byte))
	Handle    func(context.Context, []byte)
}

type Subscriber struct {
	Subscribe func(ctx context.Context, handle func(context.Context, []byte, map[string]string))
	Handle    func(context.Context, []byte, map[string]string)
}

type Handle func(context.Context, []byte, map[string]string)
type Subscribe func(context.Context, func(context.Context, []byte, map[string]string))
