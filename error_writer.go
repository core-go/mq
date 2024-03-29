package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

func NewErrorWriter(write func(ctx context.Context, model interface{}) error, modelType *reflect.Type, logError ...func(context.Context, string)) *ErrorWriter {
	return NewErrorWriterWithUnmarshal(write, modelType, json.Unmarshal, logError...)
}
func NewErrorWriterWithUnmarshal(write func(ctx context.Context, model interface{}) error, modelType *reflect.Type, unmarshal func(data []byte, v interface{}) error, logError ...func(context.Context, string)) *ErrorWriter {
	if unmarshal == nil {
		unmarshal = json.Unmarshal
	}
	h := &ErrorWriter{Write: write, ModelType: modelType, Unmarshal: unmarshal}
	if len(logError) >= 1 {
		h.LogError = logError[0]
	}
	return h
}
type ErrorWriter struct {
	Write     func(ctx context.Context, model interface{}) error
	ModelType *reflect.Type
	Unmarshal func(data []byte, v interface{}) error
	LogError  func(context.Context, string)
}

func (w *ErrorWriter) HandleError(ctx context.Context, data []byte, attrs map[string]string) error {
	if data == nil {
		return nil
	}
	if w.Write != nil {
		if w.ModelType == nil {
			return w.Write(ctx, data)
		} else {
			v := InitModel(*w.ModelType)
			err := w.Unmarshal(data, v)
			if err != nil {
				return err
			}
			return w.Write(ctx, v)
		}
	}
	if w.LogError != nil {
		if attrs == nil || len(attrs) == 0 {
			w.LogError(ctx, fmt.Sprintf("Fail to consume message: %s", data))
		} else {
			w.LogError(ctx, fmt.Sprintf("Fail to consume message: %s %s", data, attrs))
		}
	}
	return nil
}
