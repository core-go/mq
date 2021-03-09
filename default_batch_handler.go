package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

type DefaultBatchHandler struct {
	modelType  reflect.Type
	modelsType reflect.Type
	Write      func(ctx context.Context, models interface{}) ([]int, []int, error) // Return: Success indices, Fail indices, Error
	LogError   func(context.Context, string)
	LogInfo    func(context.Context, string)
}

func NewBatchHandler(modelType reflect.Type, writeBatch func(context.Context, interface{}) ([]int, []int, error), logs ...func(context.Context, string)) *DefaultBatchHandler {
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	h := &DefaultBatchHandler{modelType: modelType, modelsType: modelsType, Write: writeBatch}
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

	vs := h.initModels()
	var v = reflect.Indirect(reflect.ValueOf(vs))
	for _, message := range data {
		if message.Value != nil {
			vo := reflect.ValueOf(message.Value)
			v1 := reflect.Indirect(vo)
			v = reflect.Append(v, v1)
		} else {
			item := InitModel(h.modelType)
			err := json.Unmarshal(message.Data, item)
			if err != nil {
				failMessages = append(failMessages, message)
				return failMessages, fmt.Errorf(`cannot unmarshal item: %s. Error: %s`, message.Data, err.Error())
			}
			vo := reflect.ValueOf(item)
			x := reflect.Indirect(vo).Interface()
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
	successIndices, failIndices, er1 := h.Write(ctx, v.Interface())
	if h.LogInfo != nil {
		h.LogInfo(ctx, fmt.Sprintf(`success indices %v fail indices %v`, successIndices, failIndices))
	}
	if successIndices != nil && len(successIndices) > 0 {
		if failIndices != nil && len(failIndices) > 0 {
			for _, failIndex := range failIndices {
				failMessages = append(failMessages, data[failIndex])
			}
		}
		return failMessages, nil
	}
	if er1 != nil && h.LogError != nil {
		sv, er2 := json.Marshal(v.Interface())
		if er2 != nil {
			h.LogError(ctx, fmt.Sprintf("Cannot write batch: %s  Error: %s", v.Interface(), er1.Error()))
		} else {
			h.LogError(ctx, fmt.Sprintf("Cannot write batch: %s  Error: %s", sv, er1.Error()))
		}
	}
	return data, er1
}

func (h *DefaultBatchHandler) initModels() interface{} {
	return reflect.New(h.modelsType).Interface()
}
