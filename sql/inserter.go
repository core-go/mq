package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
)

type Inserter[T any] struct {
	db           *sql.DB
	tableName    string
	BuildParam   func(i int) string
	Map          func(T)
	BoolSupport  bool
	VersionIndex int
	schema       *Schema
	ToArray      func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewInserter[T any](db *sql.DB, tableName string, options ...func(T)) *Inserter[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlInserter[T](db, tableName, mp, nil)
}
func NewInserterWithArray[T any](db *sql.DB, tableName string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(T)) *Inserter[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlInserter[T](db, tableName, mp, toArray)
}
func NewSqlInserter[T any](db *sql.DB, tableName string, mp func(T), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *Inserter[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	driver := GetDriver(db)
	boolSupport := driver == DriverPostgres
	schema := CreateSchema(modelType)
	return &Inserter[T]{db: db, BoolSupport: boolSupport, VersionIndex: -1, schema: schema, tableName: tableName, BuildParam: buildParam, Map: mp, ToArray: toArray}
}

func (w *Inserter[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		w.Map(model)
	}
	queryInsert, values := BuildToInsertWithSchema(w.tableName, model, w.VersionIndex, w.BuildParam, w.BoolSupport, false, w.ToArray, w.schema)
	_, err := w.db.ExecContext(ctx, queryInsert, values...)
	return err
}
