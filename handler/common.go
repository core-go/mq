package handler

import (
	"context"
	"fmt"
)

type ErrorMessage struct {
	Field   string `yaml:"field" mapstructure:"field" json:"field,omitempty" gorm:"column:field" bson:"field,omitempty" dynamodbav:"field,omitempty" firestore:"field,omitempty"`
	Code    string `yaml:"code" mapstructure:"code" json:"code,omitempty" gorm:"column:code" bson:"code,omitempty" dynamodbav:"code,omitempty" firestore:"code,omitempty"`
	Param   string `yaml:"param" mapstructure:"param" json:"param,omitempty" gorm:"column:param" bson:"param,omitempty" dynamodbav:"param,omitempty" firestore:"param,omitempty"`
	Message string `yaml:"message" mapstructure:"message" json:"message,omitempty" gorm:"column:message" bson:"message,omitempty" dynamodbav:"message,omitempty" firestore:"message,omitempty"`
}

func NewErrorHandler[T any](logError ...func(context.Context, string)) *ErrorHandler[T] {
	h := &ErrorHandler[T]{}
	if len(logError) >= 1 {
		h.LogError = logError[0]
	}
	return h
}

type ErrorHandler[T any] struct {
	LogError func(context.Context, string)
}

func (w *ErrorHandler[T]) HandleError(ctx context.Context, res T, err []ErrorMessage, data []byte) error {
	if w.LogError != nil && data != nil {
		w.LogError(ctx, fmt.Sprintf("Message is invalid %s Error: %+v", data, err))
	}
	return nil
}
func (w *ErrorHandler[T]) HandleErrorWithMap(ctx context.Context, res T, err []ErrorMessage, data []byte, attrs map[string]string) error {
	if w.LogError != nil && data != nil {
		if len(attrs) > 0 {
			w.LogError(ctx, fmt.Sprintf("Message is invalid %s Attributes: %+v Error: %+v", data, attrs, err))
		} else {
			w.LogError(ctx, fmt.Sprintf("Message is invalid %s Error: %+v", data, err))
		}
	}
	return nil
}

func GetString(ctx context.Context, key string) string {
	if len(key) > 0 {
		u := ctx.Value(key)
		if u != nil {
			s, ok := u.(string)
			if ok {
				return s
			} else {
				return ""
			}
		}
	}
	return ""
}
