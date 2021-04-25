package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

type Validator struct {
	modelType reflect.Type
	check     func(ctx context.Context, model interface{}) error
}

func NewValidator(modelType reflect.Type, check func(context.Context, interface{}) error) *Validator {
	v := &Validator{modelType: modelType, check: check}
	return v
}

func (v *Validator) Validate(ctx context.Context, message *Message) error {
	item := InitModel(v.modelType)
	err := json.Unmarshal(message.Data, item)
	if err != nil {
		return fmt.Errorf(`cannot unmarshal item: %s. Error: %s`, message.Data, err.Error())
	}
	message.Value = item
	return v.check(ctx, message.Value)
}
