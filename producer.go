package mq

import "context"

type Producer interface {
	Produce(ctx context.Context, data []byte, attributes *map[string]string) (string, error)
}
