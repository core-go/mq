package mq

import (
	"context"
	"fmt"
)

func NewErrorHandler(logError ...func(context.Context, string)) *DefaultErrorHandler {
	h := &DefaultErrorHandler{}
	if len(logError) >= 1 {
		h.LogError = logError[0]
	}
	return h
}

type DefaultErrorHandler struct {
	LogError func(context.Context, string)
}

func (w *DefaultErrorHandler) HandleError(ctx context.Context, message *Message) error {
	if w.LogError != nil {
		w.LogError(ctx, fmt.Sprintf("Fail to consumer message: %s", message))
	}
	return nil
}
