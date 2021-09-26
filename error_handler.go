package mq

import (
	"context"
	"fmt"
)

func NewErrorHandler(logError ...func(context.Context, string)) *ErrorHandler {
	h := &ErrorHandler{}
	if len(logError) >= 1 {
		h.LogError = logError[0]
	}
	return h
}

type ErrorHandler struct {
	LogError func(context.Context, string)
}
type logMessage struct {
	Id         string            `json:"id,omitempty" gorm:"column:id;primary_key" bson:"id,omitempty" dynamodbav:"id,omitempty" firestore:"id,omitempty"`
	Data       []byte            `json:"data,omitempty" gorm:"column:data" bson:"data,omitempty" dynamodbav:"data,omitempty" firestore:"data,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty" gorm:"column:attributes" bson:"attributes,omitempty" dynamodbav:"attributes,omitempty" firestore:"attributes,omitempty"`
}

func (w *ErrorHandler) HandleError(ctx context.Context, data []byte, attrs map[string]string) error {
	if w.LogError != nil && data != nil {
		if attrs == nil || len(attrs) == 0 {
			w.LogError(ctx, fmt.Sprintf("Fail to consume message: %s", data))
		} else {
			w.LogError(ctx, fmt.Sprintf("Fail to consume message: %s %s", data, attrs))
		}
	}
	return nil
}
