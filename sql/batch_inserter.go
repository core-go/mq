package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
)

type BatchInserter[T any] struct {
	db           *sql.DB
	tableName    string
	BuildParam   func(i int) string
	Map          func(*T)
	Driver       string
	BoolSupport  bool
	VersionIndex int
	Schema       *Schema
	ToArray      func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
	retryAll bool
}

func NewBatchInserter[T any](db *sql.DB, tableName string, retryAll bool, options ...func(*T)) *BatchInserter[T] {
	var mp func(*T)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewSqlBatchInserter[T](db, tableName, retryAll, mp, nil)
}
func NewBatchInserterWithArray[T any](db *sql.DB, tableName string, retryAll bool, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(*T)) *BatchInserter[T] {
	var mp func(*T)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewSqlBatchInserter[T](db, tableName, retryAll, mp, toArray)
}
func NewSqlBatchInserter[T any](db *sql.DB, tableName string, retryAll bool, mp func(*T), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *BatchInserter[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() != reflect.Struct {
		panic("T must be a struct")
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
	return &BatchInserter[T]{db: db, tableName: tableName, BuildParam: buildParam, BoolSupport: boolSupport, Schema: schema, Driver: driver, Map: mp, ToArray: toArray, retryAll: retryAll}
}

func (w *BatchInserter[T]) Write(ctx context.Context, models []T) ([]int, error) {
	l := len(models)
	if l == 0 {
		return nil, nil
	}
	if w.Map != nil {
		for i := 0; i < l; i++ {
			w.Map(&models[i])
		}
	}
	query, args, err := BuildToInsertBatchWithSchema(w.tableName, models, w.Driver, w.ToArray, w.BuildParam, w.Schema)
	if err != nil {
		return nil, err
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return buildErrorArray(w.retryAll, l), err
	}
	err = tx.Commit()
	if err != nil {
		return buildErrorArray(w.retryAll, l), err
	}
	return nil, nil
}
func buildErrorArray(retryAll bool, l int) []int {
	if retryAll == false {
		return nil
	}
	failIndices := make([]int, 0)
	for i := 0; i < l; i++ {
		failIndices = append(failIndices, i)
	}
	return failIndices
}
