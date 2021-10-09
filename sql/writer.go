package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
)

type Writer struct {
	db           *sql.DB
	tableName    string
	BuildParam   func(i int) string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	BoolSupport  bool
	VersionIndex int
	driver       string
	schema       *Schema
	ToArray      func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewWriter(db *sql.DB, tableName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *Writer {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlWriter(db, tableName, modelType, mp, nil)
}
func NewWriterWithArray(db *sql.DB, tableName string, modelType reflect.Type, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(context.Context, interface{}) (interface{}, error)) *Writer {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlWriter(db, tableName, modelType, mp, toArray)
}
func NewSqlWriter(db *sql.DB, tableName string, modelType reflect.Type, mp func(context.Context, interface{}) (interface{}, error), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *Writer {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	driver := GetDriver(db)
	boolSupport := driver == DriverPostgres
	schema := CreateSchema(modelType)
	return &Writer{db: db, tableName: tableName, VersionIndex: -1, BoolSupport: boolSupport, driver: driver, schema: schema, BuildParam: buildParam, Map: mp, ToArray: toArray}
}

func (w *Writer) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		query, values, er1 := BuildToSaveWithSchema(w.tableName, m2, w.driver, w.BuildParam, w.ToArray, w.schema)
		if er1 != nil {
			return er1
		}
		_, e2 := w.db.ExecContext(ctx, query, values...)
		return e2
	}
	query, values, er3 := BuildToSaveWithSchema(w.tableName, model, w.driver, w.BuildParam, w.ToArray, w.schema)
	if er3 != nil {
		return er3
	}
	_, er4 := w.db.ExecContext(ctx, query, values...)
	return er4
}
