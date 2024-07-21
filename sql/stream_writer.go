package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
)

type StreamWriter[T any] struct {
	db         *sql.DB
	tableName  string
	BuildParam func(i int) string
	Map        func(T)
	schema     *Schema
	batchSize  int
	batch      []interface{}
	Driver     string
	ToArray    func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewStreamWriter[T any](db *sql.DB, tableName string, batchSize int, options ...func(T)) *StreamWriter[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}

	return NewSqlStreamWriter[T](db, tableName, batchSize, mp, nil)
}
func NewStreamWriterWithArray[T any](db *sql.DB, tableName string, batchSize int, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(T)) *StreamWriter[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlStreamWriter[T](db, tableName, batchSize, mp, toArray)
}
func NewSqlStreamWriter[T any](db *sql.DB, tableName string, batchSize int,
	mp func(T), toArray func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}, options ...func(i int) string) *StreamWriter[T] {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	driver := GetDriver(db)
	// boolSupport := driver == DriverPostgres
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	schema := CreateSchema(modelType)
	if len(schema.Keys) <= 0 {
		panic(fmt.Sprintf("require primary key for table '%s'", tableName))
	}
	return &StreamWriter[T]{db: db, Driver: driver, schema: schema, tableName: tableName, batchSize: batchSize, BuildParam: buildParam, Map: mp, ToArray: toArray}
}

func (w *StreamWriter[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		w.Map(model)
	}
	w.batch = append(w.batch, model)
	if len(w.batch) >= w.batchSize {
		return w.Flush(ctx)
	}
	return nil
}

func (w *StreamWriter[T]) Flush(ctx context.Context) error {
	var queryArgsArray []Statement
	for _, v := range w.batch {
		query, args, err := BuildToSaveWithArray(w.tableName, v, w.Driver, w.ToArray, w.schema)
		if err != nil {
			return err
		}
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
		w.batch = make([]interface{}, 0)
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
