package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

type Validator struct {
	modelType reflect.Type
	Check     func(ctx context.Context, model interface{}) error
	Unmarshal func([]byte, interface{}) error
}

func NewValidator(modelType reflect.Type, check func(context.Context, interface{}) error, opts...func([]byte, interface{}) error) *Validator {
	var unmarshal func([]byte, interface{}) error
	if len(opts) > 0 && opts[0] != nil {
		unmarshal = opts[0]
	} else {
		unmarshal = json.Unmarshal
	}
	v := &Validator{modelType: modelType, Check: check, Unmarshal: unmarshal}
	return v
}

func (v *Validator) Validate(ctx context.Context, message *Message) error {
	item := InitModel(v.modelType)
	err := v.Unmarshal(message.Data, item)
	if err != nil {
		return fmt.Errorf(`cannot unmarshal item: %s. Error: %s`, message.Data, err.Error())
	}
	message.Value = item
	return v.Check(ctx, message.Value)
}
