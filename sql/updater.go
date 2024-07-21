package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
)

type Updater[T any] struct {
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

func NewUpdater[T any](db *sql.DB, tableName string, options ...func(T)) *Updater[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlUpdater[T](db, tableName, mp, nil)
}
func NewUpdaterWithArray[T any](db *sql.DB, tableName string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(T)) *Updater[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlUpdater[T](db, tableName, mp, toArray)
}
func NewSqlUpdater[T any](db *sql.DB, tableName string, mp func(T), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *Updater[T] {
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
	return &Updater[T]{db: db, tableName: tableName, VersionIndex: -1, BoolSupport: boolSupport, schema: schema, BuildParam: buildParam, Map: mp, ToArray: toArray}
}

func (w *Updater[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		w.Map(model)
	}
	query, values := BuildToUpdateWithVersion(w.tableName, model, w.VersionIndex, w.BuildParam, w.BoolSupport, w.ToArray, w.schema)
	_, er2 := w.db.ExecContext(ctx, query, values...)
	return er2
}
