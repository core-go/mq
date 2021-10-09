package cassandra

import (
	"fmt"
	"reflect"
)

func BuildToInsertBatch(table string, models interface{}, options...*Schema) ([]Statement, error) {
	return BuildToInsertBatchWithVersion(table, models, -1, false, options...)
}
func BuildToInsertOrUpdateBatch(table string, models interface{}, orUpdate bool, options...*Schema) ([]Statement, error) {
	return BuildToInsertBatchWithVersion(table, models, -1, orUpdate, options...)
}
func BuildToInsertBatchWithVersion(table string, models interface{}, versionIndex int, orUpdate bool, options...*Schema) ([]Statement, error) {
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models is not a slice")
	}
	if s.Len() <= 0 {
		return nil, nil
	}
	var strt *Schema
	if len(options) > 0 {
		strt = options[0]
	} else {
		first := s.Index(0).Interface()
		modelType := reflect.TypeOf(first)
		strt = CreateSchema(modelType)
	}
	slen := s.Len()
	stmts := make([]Statement, 0)
	for j := 0; j < slen; j++ {
		model := s.Index(j).Interface()
		// mv := reflect.ValueOf(model)
		query, args := BuildToInsertWithVersion(table, model, versionIndex, orUpdate, strt)
		s := Statement{Query: query, Params: args}
		stmts = append(stmts, s)
	}
	return stmts, nil
}
func BuildToUpdateBatch(table string, models interface{}, options ...*Schema) ([]Statement, error) {
	return BuildToUpdateBatchWithVersion(table, models, -1, options...)
}
func BuildToUpdateBatchWithVersion(table string, models interface{}, versionIndex int, options ...*Schema) ([]Statement, error) {
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models is not a slice")
	}
	slen := s.Len()
	if slen <= 0 {
		return nil, nil
	}
	var strt *Schema
	if len(options) > 0 {
		strt = options[0]
	} else {
		first := s.Index(0).Interface()
		modelType := reflect.TypeOf(first)
		strt = CreateSchema(modelType)
	}
	stmts := make([]Statement, 0)
	for j := 0; j < slen; j++ {
		model := s.Index(j).Interface()
		// mv := reflect.ValueOf(model)
		query, args := BuildToUpdateWithVersion(table, model, versionIndex, strt)
		s := Statement{Query: query, Params: args}
		stmts = append(stmts, s)
	}
	return stmts, nil
}
