package mq

import "reflect"

func InitModel(modelType reflect.Type) interface{} {
	return reflect.New(modelType).Interface()
}

func InitModels(modelsType reflect.Type) interface{} {
	return reflect.New(modelsType).Interface()
}
