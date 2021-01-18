package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

type DefaultBatchHandler struct {
	modelType   reflect.Type
	modelsType  reflect.Type
	batchWriter BatchWriter
	LogError func(context.Context, string)
	LogInfo  func(context.Context, string)
}

func NewBatchHandler(modelType reflect.Type, bulkWriter BatchWriter, logs ...func(context.Context, string)) *DefaultBatchHandler {
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	h := &DefaultBatchHandler{modelType: modelType, modelsType: modelsType, batchWriter: bulkWriter}
	if len(logs) >= 1 {
		h.LogError = logs[0]
	}
	if len(logs) >= 2 {
		h.LogInfo = logs[1]
	}
	return h
}

func (h *DefaultBatchHandler) Handle(ctx context.Context, data []*Message) ([]*Message, error) {
	failMessages := make([]*Message, 0)

	var v = reflect.Indirect(reflect.ValueOf(h.initModels()))
	for _, message := range data {
		if message.Value != nil {
			v = reflect.Append(v, reflect.ValueOf(message.Value))
		} else  {
			item := InitModel(h.modelType)
			err := json.Unmarshal(message.Data, item)
			if err != nil {
				failMessages = append(failMessages, message)
				return failMessages, fmt.Errorf(`can't unmarshal item: %s. Error: %s`, string(message.Data), err.Error())
			}
			x := reflect.Indirect(reflect.ValueOf(item)).Interface()
			v = reflect.Append(v, reflect.ValueOf(x))
		}
	}
	if h.LogInfo != nil {
		m := fmt.Sprintf(`models: %v`, v)
		h.LogInfo(ctx, m)
	}
	successIndices, failIndices, err := h.batchWriter.WriteBatch(ctx, v.Interface())
	if h.LogInfo != nil {
		m := fmt.Sprintf(`success indices %v fail indices %v`, successIndices, failIndices)
		h.LogInfo(ctx, m)
	}
	if err != nil {
		if h.LogError != nil {
			m := fmt.Sprintf("Can't do bulk write: %v  Error: %s", v.Interface(), err.Error())
			h.LogError(ctx, m)
		}
		return data, err
	}
	for _, failIndex := range failIndices {
		failMessages = append(failMessages, data[failIndex])
	}

	return failMessages, nil
}

func (h *DefaultBatchHandler) initModels() interface{} {
	return 	reflect.New(h.modelsType).Interface()
}
