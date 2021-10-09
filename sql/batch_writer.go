package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
)

type BatchWriter struct {
	db           *sql.DB
	tableName    string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	Schema       *Schema
	ToArray      func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}
func NewBatchWriter(db *sql.DB, tableName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *BatchWriter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewBatchWriterWithMap(db, tableName, modelType, mp, nil)
}
func NewBatchWriterWithMap(db *sql.DB, tableName string, modelType reflect.Type, mp func(context.Context, interface{}) (interface{}, error), options... func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}) *BatchWriter {
	var toArray func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
	if len(options) > 0 {
		toArray = options[0]
	}
	schema := CreateSchema(modelType)
	return &BatchWriter{db: db, tableName: tableName, Schema: schema, Map: mp, ToArray: toArray}
}

func (w *BatchWriter) Write(ctx context.Context, models interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)
	var models2 interface{}
	var er0 error
	if w.Map != nil {
		models2, er0 = MapModels(ctx, models, w.Map)
		if er0 != nil {
			s0 := reflect.ValueOf(models2)
			_, er0b := InterfaceSlice(models2)
			failIndices = ToArrayIndex(s0, failIndices)
			return successIndices, failIndices, er0b
		}
	} else {
		models2 = models
	}
	s := reflect.ValueOf(models2)
	_, er2 := SaveBatchWithArray(ctx, w.db, w.tableName, models2, w.ToArray)

	if er2 == nil {
		// Return full success
		successIndices = ToArrayIndex(s, successIndices)
		return successIndices, failIndices, er2
	} else {
		// Return full fail
		failIndices = ToArrayIndex(s, failIndices)
	}
	return successIndices, failIndices, er2
}
