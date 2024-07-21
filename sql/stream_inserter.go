package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
)

type StreamInserter[T any] struct {
	db         *sql.DB
	tableName  string
	BuildParam func(i int) string
	Map        func(T)
	Driver     string
	schema     *Schema
	batchSize  int
	batch      []interface{}
	ToArray    func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewStreamInserter[T any](db *sql.DB, tableName string, batchSize int, options ...func(T)) *StreamInserter[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}

	return NewSqlStreamInserter[T](db, tableName, batchSize, mp, nil)
}
func NewStreamInserterWithArray[T any](db *sql.DB, tableName string, batchSize int, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(T)) *StreamInserter[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlStreamInserter[T](db, tableName, batchSize, mp, toArray)
}
func NewSqlStreamInserter[T any](db *sql.DB, tableName string, batchSize int,
	mp func(T), toArray func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}, options ...func(i int) string) *StreamInserter[T] {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	driver := GetDriver(db)
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	schema := CreateSchema(modelType)
	return &StreamInserter[T]{db: db, Driver: driver, schema: schema, tableName: tableName, batchSize: batchSize, BuildParam: buildParam, Map: mp, ToArray: toArray}
}

func (w *StreamInserter[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		w.Map(model)
	}
	w.batch = append(w.batch, model)
	if len(w.batch) >= w.batchSize {
		return w.Flush(ctx)
	}
	return nil
}

func (w *StreamInserter[T]) Flush(ctx context.Context) error {
	query, args, err := BuildToInsertBatchWithSchema(w.tableName, w.batch, w.Driver, w.ToArray, w.BuildParam, w.schema)
	if err != nil {
		return err
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	// Defer a rollback in case anything fails.
	defer func() {
		w.batch = make([]interface{}, 0)
	}()
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	return tx.Commit()
}
