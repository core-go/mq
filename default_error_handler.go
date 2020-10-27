package mq

import (
	"context"
)

func NewErrorHandler() *DefaultErrorHandler {
	return &DefaultErrorHandler{}
}

type DefaultErrorHandler struct {
}

func (w *DefaultErrorHandler) HandleError(ctx context.Context, message *Message) error {
	Errorf(ctx, "Fail after all retries: %v", message)
	return nil
}
