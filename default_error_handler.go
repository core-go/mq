package mq

import (
	"context"
	"github.com/sirupsen/logrus"
)

func NewErrorHandler() *DefaultErrorHandler {
	return &DefaultErrorHandler{}
}

type DefaultErrorHandler struct {
}

func (w *DefaultErrorHandler) HandleError(ctx context.Context, message *Message) error {
	logrus.Errorf("Fail after all retries: %v", message)
	return nil
}
