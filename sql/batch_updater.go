package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
)

type BatchUpdater struct {
	db           *sql.DB
	tableName    string
	BuildParam   func(i int) string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	BoolSupport  bool
	VersionIndex int
	Schema       *Schema
	ToArray      func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}
func NewBatchUpdater(db *sql.DB, tableName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *BatchUpdater {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewSqlBatchUpdater(db, tableName, modelType, -1, mp, nil)
}
func NewBatchUpdaterWithArray(db *sql.DB, tableName string, modelType reflect.Type, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(context.Context, interface{}) (interface{}, error)) *BatchUpdater {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewSqlBatchUpdater(db, tableName, modelType, -1, mp, toArray)
}
func NewBatchUpdaterWithVersion(db *sql.DB, tableName string, modelType reflect.Type, versionIndex int, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(context.Context, interface{}) (interface{}, error)) *BatchUpdater {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewSqlBatchUpdater(db, tableName, modelType, versionIndex, mp, toArray)
}
func NewSqlBatchUpdater(db *sql.DB, tableName string, modelType reflect.Type, versionIndex int, mp func(context.Context, interface{}) (interface{}, error), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *BatchUpdater {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	driver := GetDriver(db)
	boolSupport := driver == DriverPostgres
	schema := CreateSchema(modelType)
	return &BatchUpdater{db: db, tableName: tableName, Schema: schema, BoolSupport: boolSupport, VersionIndex: versionIndex, Map: mp, BuildParam: buildParam, ToArray: toArray}
}
func (w *BatchUpdater) Write(ctx context.Context, models interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)
	var models2 interface{}
	var er0 error
	if w.Map != nil {
		models2, er0 = MapModels(ctx, models, w.Map)
		if er0 != nil {
			s0 := reflect.ValueOf(models2)
			_, er0b := InterfaceSlice(models2)
			failIndices = ToArrayIndex(s0, failIndices)
			return successIndices, failIndices, er0b
		}
	} else {
		models2 = models
	}
	_, err := UpdateBatchWithVersion(ctx, w.db, w.tableName, models2, w.VersionIndex, w.ToArray, w.BuildParam, w.BoolSupport, w.Schema)
	s := reflect.ValueOf(models)
	if err == nil {
		// Return full success
		successIndices = ToArrayIndex(s, successIndices)
		return successIndices, failIndices, err
	} else {
		// Return full fail
		failIndices = ToArrayIndex(s, failIndices)
	}
	return successIndices, failIndices, err
}
