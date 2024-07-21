package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
)

type Writer[T any] struct {
	db          *sql.DB
	tableName   string
	BuildParam  func(i int) string
	Map         func(T)
	BoolSupport bool
	schema      *Schema
	Driver      string
	ToArray     func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewWriterWithMap[T any](db *sql.DB, tableName string, mp func(T), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *Writer[T] {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	driver := GetDriver(db)
	boolSupport := driver == DriverPostgres
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	schema := CreateSchema(modelType)
	if len(schema.Keys) <= 0 {
		panic(fmt.Sprintf("require primary key for table '%s'", tableName))
	}
	return &Writer[T]{db: db, tableName: tableName, BuildParam: buildParam, Map: mp, BoolSupport: boolSupport, schema: schema, Driver: driver, ToArray: toArray}
}

func NewWriter[T any](db *sql.DB, tableName string, opts ...func(T)) *Writer[T] {
	var mp func(T)
	if len(opts) >= 1 {
		mp = opts[0]
	}
	return NewWriterWithMap[T](db, tableName, mp, nil)
}

func (w *Writer[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		w.Map(model)
	}
	query, args, err := BuildToSaveWithSchema(w.tableName, model, w.Driver, w.BuildParam, w.ToArray, w.schema)
	if err != nil {
		return err
	}
	_, er2 := w.db.ExecContext(ctx, query, args...)
	return er2
}
