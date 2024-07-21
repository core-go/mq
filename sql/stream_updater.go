package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
)

type StreamUpdater[T any] struct {
	db           *sql.DB
	tableName    string
	BuildParam   func(i int) string
	Map          func(T)
	BoolSupport  bool
	VersionIndex int
	schema       *Schema
	batchSize    int
	batch        []T
	ToArray      func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewStreamUpdater[T any](db *sql.DB, tableName string, modelType reflect.Type, batchSize int, options ...func(T)) *StreamUpdater[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}

	return NewSqlStreamUpdater[T](db, tableName, modelType, batchSize, mp, nil)
}
func NewStreamUpdaterWithArray[T any](db *sql.DB, tableName string, modelType reflect.Type, batchSize int, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(T)) *StreamUpdater[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlStreamUpdater[T](db, tableName, modelType, batchSize, mp, toArray)
}
func NewSqlStreamUpdater[T any](db *sql.DB, tableName string, modelType reflect.Type, batchSize int,
	mp func(T), toArray func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}, options ...func(i int) string) *StreamUpdater[T] {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	driver := GetDriver(db)
	boolSupport := driver == DriverPostgres
	schema := CreateSchema(modelType)
	if len(schema.Keys) <= 0 {
		panic(fmt.Sprintf("require primary key for table '%s'", tableName))
	}
	return &StreamUpdater[T]{db: db, BoolSupport: boolSupport, VersionIndex: -1, schema: schema, tableName: tableName, batchSize: batchSize, BuildParam: buildParam, Map: mp, ToArray: toArray}
}

func (w *StreamUpdater[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		w.Map(model)
	}
	w.batch = append(w.batch, model)
	if len(w.batch) >= w.batchSize {
		return w.Flush(ctx)
	}
	return nil
}

func (w *StreamUpdater[T]) Flush(ctx context.Context) error {
	var queryArgsArray []Statement
	for _, v := range w.batch {
		query, args := BuildToUpdateWithArray(w.tableName, v, w.BuildParam, w.BoolSupport, w.ToArray, w.schema)
		queryArgs := Statement{
			Query:  query,
			Params: args,
		}
		queryArgsArray = append(queryArgsArray, queryArgs)
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		w.batch = make([]T, 0)
	}()

	for _, v := range queryArgsArray {
		_, err = tx.Exec(v.Query, v.Params...)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}

	return nil
}
