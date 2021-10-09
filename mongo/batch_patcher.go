package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"reflect"
)

type BatchPatcher struct {
	collection *mongo.Collection
	IdName     string
	modelType  reflect.Type
	modelsType reflect.Type
}

func NewBatchPatcherWithId(database *mongo.Database, collectionName string, modelType reflect.Type, fieldName string) *BatchPatcher {
	if len(fieldName) == 0 {
		_, idName, _ := FindIdField(modelType)
		fieldName = idName
	}
	return CreateMongoBatchPatcherIdName(database, collectionName, modelType, fieldName)
}

func NewBatchPatcher(database *mongo.Database, collectionName string, modelType reflect.Type) *BatchPatcher {
	return CreateMongoBatchPatcherIdName(database, collectionName, modelType, "")
}

func CreateMongoBatchPatcherIdName(database *mongo.Database, collectionName string, modelType reflect.Type, fieldName string) *BatchPatcher {
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	collection := database.Collection(collectionName)
	return &BatchPatcher{collection, fieldName, modelType, modelsType}
}

func (w *BatchPatcher) Write(ctx context.Context, models []map[string]interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)

	s := reflect.ValueOf(models)
	_, err := PatchMaps(ctx, w.collection, models, w.IdName)

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
