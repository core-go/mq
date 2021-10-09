package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"reflect"
)

type BatchUpdater struct {
	collection *mongo.Collection
	IdName     string
	modelType  reflect.Type
	modelsType reflect.Type
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewBatchUpdaterWithId(database *mongo.Database, collectionName string, modelType reflect.Type, fieldName string, options...func(context.Context, interface{}) (interface{}, error)) *BatchUpdater {
	if len(fieldName) == 0 {
		_, idName, _ := FindIdField(modelType)
		fieldName = idName
	}
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	collection := database.Collection(collectionName)
	return &BatchUpdater{collection, fieldName, modelType, modelsType, mp}
}

func NewBatchUpdater(database *mongo.Database, collectionName string, modelType reflect.Type, options...func(context.Context, interface{}) (interface{}, error)) *BatchUpdater {
	return NewBatchUpdaterWithId(database, collectionName, modelType, "", options...)
}

func (w *BatchUpdater) Write(ctx context.Context, models interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)

	s := reflect.ValueOf(models)
	var err error
	if w.Map != nil {
		m2, er0 := MapModels(ctx, models, w.Map)
		if er0 != nil {
			return successIndices, failIndices, er0
		}
		_, err = UpdateMany(ctx, w.collection, m2, w.IdName)
	} else {
		_, err = UpdateMany(ctx, w.collection, models, w.IdName)
	}

	if err == nil {
		// Return full success
		for i := 0; i < s.Len(); i++ {
			successIndices = append(successIndices, i)
		}
		return successIndices, failIndices, err
	}

	if bulkWriteException, ok := err.(mongo.BulkWriteException); ok {
		for _, writeError := range bulkWriteException.WriteErrors {
			failIndices = append(failIndices, writeError.Index)
		}
		for i := 0; i < s.Len(); i++ {
			if !InArray(i, failIndices) {
				successIndices = append(successIndices, i)
			}
		}
	} else {
		// Return full fail
		for i := 0; i < s.Len(); i++ {
			failIndices = append(failIndices, i)
		}
	}
	return successIndices, failIndices, err
}
