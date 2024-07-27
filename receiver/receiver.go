package receiver

import "context"

type SimpleReceiver struct {
	Receive func(ctx context.Context, handle func(context.Context, []byte))
	Handle  func(context.Context, []byte)
}

type Receiver struct {
	Receive func(ctx context.Context, handle func(context.Context, []byte, map[string]string))
	Handle  func(context.Context, []byte, map[string]string)
}

type Handle func(context.Context, []byte, map[string]string)
type Receive func(context.Context, func(context.Context, []byte, map[string]string))
