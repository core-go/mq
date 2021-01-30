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
	LogError    func(context.Context, string)
	LogInfo     func(context.Context, string)
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
		} else {
			item := InitModel(h.modelType)
			err := json.Unmarshal(message.Data, item)
			if err != nil {
				failMessages = append(failMessages, message)
				return failMessages, fmt.Errorf(`cannot unmarshal item: %s. Error: %s`, message.Data, err.Error())
			}
			x := reflect.Indirect(reflect.ValueOf(item)).Interface()
			v = reflect.Append(v, reflect.ValueOf(x))
		}
	}
	if h.LogInfo != nil {
		sv, er0 := json.Marshal(v.Interface())
		if er0 != nil {
			h.LogInfo(ctx, fmt.Sprintf(`models: %s`, v))
		} else {
			h.LogInfo(ctx, fmt.Sprintf(`models: %s`, sv))
		}
	}
	successIndices, failIndices, er1 := h.batchWriter.WriteBatch(ctx, v.Interface())
	if h.LogInfo != nil {
		h.LogInfo(ctx, fmt.Sprintf(`success indices %v fail indices %v`, successIndices, failIndices))
	}
	if er1 != nil {
		if h.LogError != nil {
			sv, er2 := json.Marshal(v.Interface())
			if er2 != nil {
				h.LogError(ctx, fmt.Sprintf("Cannot write batch: %s  Error: %s", v.Interface(), er1.Error()))
			} else {
				h.LogError(ctx, fmt.Sprintf("Cannot write batch: %s  Error: %s", sv, er1.Error()))
			}
		}
		return data, er1
	}
	for _, failIndex := range failIndices {
		failMessages = append(failMessages, data[failIndex])
	}

	return failMessages, nil
}

func (h *DefaultBatchHandler) initModels() interface{} {
	return reflect.New(h.modelsType).Interface()
}
