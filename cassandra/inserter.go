package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	"reflect"
)

type Inserter struct {
	session      *gocql.Session
	tableName    string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	schema       *Schema
	VersionIndex int
}

func NewInserterWithMap(session *gocql.Session, tableName string, modelType reflect.Type, mp func(context.Context, interface{}) (interface{}, error), options ...int) *Inserter {
	versionIndex := -1
	if len(options) > 0 && options[0] >= 0 {
		versionIndex = options[0]
	}
	schema := CreateSchema(modelType)
	return &Inserter{session: session, tableName: tableName, Map: mp, schema: schema, VersionIndex: versionIndex}
}

func NewInserter(session *gocql.Session, tableName string, modelType reflect.Type, options ...func(ctx context.Context, model interface{}) (interface{}, error)) *Inserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewInserterWithMap(session, tableName, modelType, mp)
}

func (w *Inserter) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		_, err := InsertWithVersion(w.session, w.tableName, m2, w.VersionIndex, w.schema)
		return err
	}
	_, err := InsertWithVersion(w.session, w.tableName, model, w.VersionIndex, w.schema)
	return err
}
