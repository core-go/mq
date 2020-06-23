package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/common-go/validator"
	"github.com/sirupsen/logrus"
	"reflect"
)

type DefaultValidator struct {
	modelType reflect.Type
	va        validator.Validator
}

func NewValidator(modelType reflect.Type, va validator.Validator) *DefaultValidator {
	return &DefaultValidator{modelType, va}
}

func (v *DefaultValidator) Validate(ctx context.Context, message *Message) error {
	item := InitModel(v.modelType)
	err := json.Unmarshal(message.Data, item)
	if err != nil {
		return fmt.Errorf(`can't unmarshal item: %v. Error: %s`, string(message.Data), err.Error())
	}
	message.Value = reflect.Indirect(reflect.ValueOf(item)).Interface()
	errorMessages, err := v.va.Validate(ctx, message.Value)
	if err != nil {
		logrus.Errorf("Validate error: %s", err.Error())
		return err
	}
	if len(errorMessages) > 0 {
		logrus.Errorf("errorMessages: %v", errorMessages, err)
		return fmt.Errorf("data invalid: %v", errorMessages)
	}
	return nil
}
