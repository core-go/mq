package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
)

type Updater struct {
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
func NewUpdater(db *sql.DB, tableName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *Updater {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlUpdater(db, tableName, modelType, mp, nil)
}
func NewUpdaterWithArray(db *sql.DB, tableName string, modelType reflect.Type, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(context.Context, interface{}) (interface{}, error)) *Updater {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlUpdater(db, tableName, modelType, mp, toArray)
}
func NewSqlUpdater(db *sql.DB, tableName string, modelType reflect.Type, mp func(context.Context, interface{}) (interface{}, error), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *Updater {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	driver := GetDriver(db)
	boolSupport := driver == DriverPostgres
	schema := CreateSchema(modelType)
	return &Updater{db: db, tableName: tableName, VersionIndex: -1, BoolSupport: boolSupport, schema: schema, BuildParam: buildParam, Map: mp, ToArray: toArray}
}

func (w *Updater) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		query0, values0 := BuildToUpdateWithVersion(w.tableName, m2, w.VersionIndex, w.BuildParam, w.BoolSupport, w.ToArray, w.schema)
		_, er1 := w.db.ExecContext(ctx, query0, values0...)
		return er1
	}
	query, values := BuildToUpdateWithVersion(w.tableName, model, w.VersionIndex, w.BuildParam, w.BoolSupport, w.ToArray, w.schema)
	_, er2 := w.db.ExecContext(ctx, query, values...)
	return er2
}
