package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"reflect"
)

type BatchWriter struct {
	collection *mongo.Collection
	IdName     string
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewBatchWriterWithId(database *mongo.Database, collectionName string, modelType reflect.Type, fieldName string, options...func(context.Context, interface{}) (interface{}, error)) *BatchWriter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	if len(fieldName) == 0 {
		_, idName, _ := FindIdField(modelType)
		fieldName = idName
	}
	collection := database.Collection(collectionName)
	return &BatchWriter{collection, fieldName, mp}
}
func NewBatchWriter(database *mongo.Database, collectionName string, modelType reflect.Type, options...func(context.Context, interface{}) (interface{}, error)) *BatchWriter {
	return NewBatchWriterWithId(database, collectionName, modelType, "", options...)
}
func (w *BatchWriter) Write(ctx context.Context, models interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)

	s := reflect.ValueOf(models)
	var err error
	if w.Map != nil {
		m2, er0 := MapModels(ctx, models, w.Map)
		if er0 != nil {
			return successIndices, failIndices, er0
		}
		_, err = UpsertMany(ctx, w.collection, m2, w.IdName)
	} else {
		_, err = UpsertMany(ctx, w.collection, models, w.IdName)
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
