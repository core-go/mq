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
}

func NewBatchHandler(modelType reflect.Type, bulkWriter BatchWriter) *DefaultBatchHandler {
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	return &DefaultBatchHandler{modelType, modelsType, bulkWriter}
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
				return failMessages, fmt.Errorf(`can't unmarshal item: %v. Error: %s`, string(message.Data), err.Error())
			}
			x := reflect.Indirect(reflect.ValueOf(item)).Interface()
			v = reflect.Append(v, reflect.ValueOf(x))
		}
	}
	if IsDebugEnabled() {
		Debugf(ctx, `models: %v`, v)
	}
	successIndices, failIndices, err := h.batchWriter.WriteBatch(ctx, v.Interface())
	if IsDebugEnabled() {
		Debugf(ctx, `successIndices %v failIndices %v`, successIndices, failIndices)
	}
	if err != nil {
		Errorf(ctx, "Can't do bulk write: %v  Error: %s", v.Interface(), err.Error())
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
