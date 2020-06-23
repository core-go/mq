package mq

import "context"

type ErrorHandler interface {
	HandleError(ctx context.Context, message *Message) error
}
