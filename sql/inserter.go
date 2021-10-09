package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
)

type Inserter struct {
	db           *sql.DB
	tableName    string
	BuildParam   func(i int) string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	BoolSupport  bool
	VersionIndex int
	schema       *Schema
	ToArray      func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewInserter(db *sql.DB, tableName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *Inserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlInserter(db, tableName, modelType, mp, nil)
}
func NewInserterWithArray(db *sql.DB, tableName string, modelType reflect.Type, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(context.Context, interface{}) (interface{}, error)) *Inserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlInserter(db, tableName, modelType, mp, toArray)
}
func NewSqlInserter(db *sql.DB, tableName string, modelType reflect.Type, mp func(context.Context, interface{}) (interface{}, error), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *Inserter {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	driver := GetDriver(db)
	boolSupport := driver == DriverPostgres
	schema := CreateSchema(modelType)
	return &Inserter{db: db, BoolSupport: boolSupport, VersionIndex: -1, schema: schema, tableName: tableName, BuildParam: buildParam, Map: mp, ToArray: toArray}
}

func (w *Inserter) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}

		queryInsert, values := BuildToInsertWithSchema(w.tableName, m2, w.VersionIndex, w.BuildParam, w.BoolSupport, false, w.ToArray, w.schema)
		_, err := w.db.ExecContext(ctx, queryInsert, values...)
		return err
	}
	queryInsert, values := BuildToInsertWithSchema(w.tableName, model, w.VersionIndex, w.BuildParam, w.BoolSupport, false, w.ToArray, w.schema)
	_, err := w.db.ExecContext(ctx, queryInsert, values...)
	return err
}
