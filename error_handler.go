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
type logMessage struct {
	Id         string            `json:"id,omitempty" gorm:"column:id;primary_key" bson:"id,omitempty" dynamodbav:"id,omitempty" firestore:"id,omitempty"`
	Data       []byte            `json:"data,omitempty" gorm:"column:data" bson:"data,omitempty" dynamodbav:"data,omitempty" firestore:"data,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty" gorm:"column:attributes" bson:"attributes,omitempty" dynamodbav:"attributes,omitempty" firestore:"attributes,omitempty"`
}

func (w *DefaultErrorHandler) HandleError(ctx context.Context, message *Message) error {
	if w.LogError != nil && message != nil {
		l := logMessage{Id: message.Id, Data: message.Data, Attributes: message.Attributes}
		w.LogError(ctx, fmt.Sprintf("Fail to consume message: %s", l))
	}
	return nil
}
