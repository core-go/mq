package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	"reflect"
)

type BatchUpdater struct {
	session      *gocql.Session
	tableName    string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	VersionIndex int
	Schema       *Schema
}
func NewBatchUpdater(session *gocql.Session, tableName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *BatchUpdater {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewBatchUpdaterWithVersion(session, tableName, modelType, mp)
}
func NewBatchUpdaterWithVersion(session *gocql.Session, tableName string, modelType reflect.Type, mp func(context.Context, interface{}) (interface{}, error), options...int) *BatchUpdater {
	versionIndex := -1
	if len(options) > 0 && options[0] >= 0 {
		versionIndex = options[0]
	}
	schema := CreateSchema(modelType)
	return &BatchUpdater{session: session, tableName: tableName, Schema: schema, VersionIndex: versionIndex, Map: mp}
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
	_, err := UpdateBatchWithVersion(ctx, w.session, w.tableName, models2, w.VersionIndex, w.Schema)
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
