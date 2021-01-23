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
	va        validator.Validator
	LogError  func(context.Context, string)
}

func NewValidator(modelType reflect.Type, va validator.Validator, logError ...func(context.Context, string)) *DefaultValidator {
	v := &DefaultValidator{modelType: modelType, va: va}
	if len(logError) >= 1 {
		v.LogError = logError[0]
	}
	return v
}

func (v *DefaultValidator) Validate(ctx context.Context, message *Message) error {
	item := InitModel(v.modelType)
	err := json.Unmarshal(message.Data, item)
	if err != nil {
		return fmt.Errorf(`can't unmarshal item: %s. Error: %s`, string(message.Data), err.Error())
	}
	message.Value = reflect.Indirect(reflect.ValueOf(item)).Interface()
	errorMessages, err := v.va.Validate(ctx, message.Value)
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
