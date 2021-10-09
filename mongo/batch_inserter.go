package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"reflect"
)

type BatchInserter struct {
	collection *mongo.Collection
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewBatchInserter(database *mongo.Database, collectionName string, options...func(context.Context, interface{}) (interface{}, error)) *BatchInserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	collection := database.Collection(collectionName)
	return &BatchInserter{collection: collection, Map: mp}
}

func (w *BatchInserter) Write(ctx context.Context, models interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)
	s := reflect.ValueOf(models)
	var er1 error
	if w.Map != nil {
		m2, er0 := MapModels(ctx, models, w.Map)
		if er0 != nil {
			return successIndices, failIndices, er0
		}
		_, _, er1 = InsertManySkipErrors(ctx, w.collection, m2)
	} else {
		_, _, er1 = InsertManySkipErrors(ctx, w.collection, models)
	}

	if er1 == nil {
		// Return full success
		for i := 0; i < s.Len(); i++ {
			successIndices = append(successIndices, i)
		}
		return successIndices, failIndices, er1
	}

	if bulkWriteException, ok := er1.(mongo.BulkWriteException); ok {
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
	return successIndices, failIndices, er1
}
