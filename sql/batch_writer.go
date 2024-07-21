package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
)

type BatchWriter[T any] struct {
	db        *sql.DB
	tableName string
	Map       func(*T)
	Driver    string
	Schema    *Schema
	ToArray   func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
	retryAll bool
}

func NewBatchWriter[T any](db *sql.DB, tableName string, retryAll bool, options ...func(*T)) *BatchWriter[T] {
	var mp func(*T)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewBatchWriterWithArray[T](db, tableName, retryAll, nil, mp)
}
func NewBatchWriterWithArray[T any](db *sql.DB, tableName string, retryAll bool, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(*T)) *BatchWriter[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() != reflect.Struct {
		panic("T must be a struct")
	}
	driver := GetDriver(db)
	var mp func(*T)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	schema := CreateSchema(modelType)
	if len(schema.Keys) <= 0 {
		panic(fmt.Sprintf("require primary key for table '%s'", tableName))
	}
	return &BatchWriter[T]{db: db, tableName: tableName, Schema: schema, Driver: driver, Map: mp, ToArray: toArray, retryAll: retryAll}
}

func (w *BatchWriter[T]) Write(ctx context.Context, models []T) ([]int, error) {
	l := len(models)
	if l == 0 {
		return nil, nil
	}
	if w.Map != nil {
		for i := 0; i < l; i++ {
			w.Map(&models[i])
		}
	}
	var queryArgsArray []Statement
	for _, v := range models {
		query, args, err := BuildToSaveWithArray(w.tableName, v, w.Driver, w.ToArray, w.Schema)
		if err != nil {
			return nil, err
		}
		queryArgs := Statement{
			Query:  query,
			Params: args,
		}
		queryArgsArray = append(queryArgsArray, queryArgs)
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	for _, v := range queryArgsArray {
		_, err = tx.Exec(v.Query, v.Params...)
		if err != nil {
			return buildErrorArray(w.retryAll, l), err
		}
	}

	err = tx.Commit()
	if err != nil {
		return buildErrorArray(w.retryAll, l), err
	}

	return nil, nil
}
