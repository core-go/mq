package mq

import "reflect"

func InitModel(modelType reflect.Type) interface{} {
	return reflect.New(modelType).Interface()
}
func InitModelValue(modelType reflect.Type) interface{} {
	return reflect.New(modelType)
}
