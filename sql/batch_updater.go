package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
)

type BatchUpdater[T any] struct {
	db           *sql.DB
	tableName    string
	BuildParam   func(i int) string
	Map          func(*T)
	BoolSupport  bool
	VersionIndex int
	Schema       *Schema
	ToArray      func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
	retryAll bool
}

func NewBatchUpdater[T any](db *sql.DB, tableName string, retryAll bool, options ...func(*T)) *BatchUpdater[T] {
	var mp func(*T)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewSqlBatchUpdater[T](db, tableName, retryAll, -1, mp, nil)
}
func NewBatchUpdaterWithArray[T any](db *sql.DB, tableName string, retryAll bool, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(*T)) *BatchUpdater[T] {
	var mp func(*T)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewSqlBatchUpdater[T](db, tableName, retryAll, -1, mp, toArray)
}
func NewBatchUpdaterWithVersion[T any](db *sql.DB, tableName string, retryAll bool, versionIndex int, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(*T)) *BatchUpdater[T] {
	var mp func(*T)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewSqlBatchUpdater[T](db, tableName, retryAll, versionIndex, mp, toArray)
}
func NewSqlBatchUpdater[T any](db *sql.DB, tableName string, retryAll bool, versionIndex int, mp func(*T), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *BatchUpdater[T] {
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
	if len(schema.Keys) <= 0 {
		panic(fmt.Sprintf("require primary key for table '%s'", tableName))
	}
	return &BatchUpdater[T]{db: db, tableName: tableName, Schema: schema, BoolSupport: boolSupport, VersionIndex: versionIndex, Map: mp, BuildParam: buildParam, ToArray: toArray, retryAll: retryAll}
}
func (w *BatchUpdater[T]) Write(ctx context.Context, models []T) ([]int, error) {
	l := len(models)
	if l == 0 {
		return nil, nil
	}
	if w.Map != nil {
		for i := 0; i < l; i++ {
			w.Map(&models[i])
		}
	}
	var queryArgsArray []Statement
	for _, v := range models {
		query, args := BuildToUpdateWithArray(w.tableName, v, w.BuildParam, w.BoolSupport, w.ToArray, w.Schema)
		queryArgs := Statement{
			Query:  query,
			Params: args,
		}
		queryArgsArray = append(queryArgsArray, queryArgs)
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	for _, v := range queryArgsArray {
		_, err = tx.Exec(v.Query, v.Params...)
		if err != nil {
			return buildErrorArray(w.retryAll, l), err
		}
	}

	err = tx.Commit()
	if err != nil {
		return buildErrorArray(w.retryAll, l), err
	}

	return nil, nil
}
