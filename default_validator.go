package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/common-go/validator"
	"reflect"
)

type DefaultValidator struct {
	modelType reflect.Type
	validate  func(ctx context.Context, model interface{}) ([]validator.ErrorMessage, error)
	LogError  func(context.Context, string)
}

func NewValidator(modelType reflect.Type, validate func(context.Context, interface{}) ([]validator.ErrorMessage, error), logError ...func(context.Context, string)) *DefaultValidator {
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
	message.Value = reflect.Indirect(reflect.ValueOf(item)).Interface()
	errorMessages, err := v.validate(ctx, message.Value)
	if err != nil {
		if v.LogError != nil {
			v.LogError(ctx, "Validate error: "+err.Error())
		}
		return err
	}
	if len(errorMessages) > 0 {
		m := fmt.Sprintf("invalid data: %s", errorMessages)
		if v.LogError != nil {
			v.LogError(ctx, m)
		}
		return fmt.Errorf(m)
	}
	return nil
}
