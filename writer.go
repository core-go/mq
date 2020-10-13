package mq

import "context"

type Writer interface {
	Write(ctx context.Context, model interface{}) error
}
