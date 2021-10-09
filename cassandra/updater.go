package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	"reflect"
)

type Updater struct {
	session      *gocql.Session
	tableName    string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	VersionIndex int
	schema       *Schema
}

func NewUpdater(session *gocql.Session, tableName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *Updater {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewUpdaterWithVersion(session, tableName, modelType, mp)
}
func NewUpdaterWithVersion(session *gocql.Session, tableName string, modelType reflect.Type, mp func(context.Context, interface{}) (interface{}, error), options ...int) *Updater {
	version := -1
	if len(options) > 0 && options[0] >= 0 {
		version = options[0]
	}
	schema := CreateSchema(modelType)
	return &Updater{session: session, tableName: tableName, VersionIndex: version, schema: schema, Map: mp}
}

func (w *Updater) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		_, er1 := UpdateWithVersion(w.session, w.tableName, m2, w.VersionIndex, w.schema)
		return er1
	}
	_, er2 := UpdateWithVersion(w.session, w.tableName, model, w.VersionIndex, w.schema)
	return er2
}
