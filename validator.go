package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

type DefaultValidator struct {
	modelType reflect.Type
	validate  func(ctx context.Context, model interface{}) error
	LogError  func(context.Context, string)
}

func NewValidator(modelType reflect.Type, validate func(context.Context, interface{}) error, logError ...func(context.Context, string)) *DefaultValidator {
	v := &DefaultValidator{modelType: modelType, validate: validate}
	if len(logError) >= 1 {
		v.LogError = logError[0]
	}
	return v
}

func (v *DefaultValidator) Validate(ctx context.Context, message *Message) error {
	item := InitModel(v.modelType)
	err := json.Unmarshal(message.Data, item)
	if err != nil {
		return fmt.Errorf(`cannot unmarshal item: %s. Error: %s`, message.Data, err.Error())
	}
	message.Value = item
	return v.validate(ctx, message.Value)
}
