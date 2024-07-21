package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
)

func BuildToUpdateBatch(table string, models interface{}, buildParam func(int) string, options ...*Schema) ([]Statement, error) {
	return BuildToUpdateBatchWithVersion(table, models, -1, buildParam, false, nil, options...)
}
func BuildToUpdateBatchWithBool(table string, models interface{}, buildParam func(int) string, boolSupport bool, options ...*Schema) ([]Statement, error) {
	return BuildToUpdateBatchWithVersion(table, models, -1, buildParam, boolSupport, nil, options...)
}
func BuildToUpdateBatchWithArray(table string, models interface{}, buildParam func(int) string, boolSupport bool, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) ([]Statement, error) {
	return BuildToUpdateBatchWithVersion(table, models, -1, buildParam, boolSupport, toArray, options...)
}
func BuildToUpdateBatchWithVersion(table string, models interface{}, versionIndex int, buildParam func(int) string, boolSupport bool, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) ([]Statement, error) {
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models must be a slice")
	}
	slen := s.Len()
	if slen <= 0 {
		return nil, nil
	}
	var strt *Schema
	if len(options) > 0 && options[0] != nil {
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
		query, args := BuildToUpdateWithVersion(table, model, versionIndex, buildParam, boolSupport, toArray, strt)
		s := Statement{Query: query, Params: args}
		stmts = append(stmts, s)
	}
	return stmts, nil
}
func BuildToInsertBatch(table string, models interface{}, driver string, options ...*Schema) (string, []interface{}, error) {
	buildParam := GetBuildByDriver(driver)
	return BuildToInsertBatchWithSchema(table, models, driver, nil, buildParam, options...)
}
func BuildToInsertBatchWithArray(table string, models interface{}, driver string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) (string, []interface{}, error) {
	buildParam := GetBuildByDriver(driver)
	return BuildToInsertBatchWithSchema(table, models, driver, toArray, buildParam, options...)
}
func BuildToInsertBatchWithSchema(table string, models interface{}, driver string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, buildParam func(int) string, options ...*Schema) (string, []interface{}, error) {
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return "", nil, fmt.Errorf("models must be a slice")
	}
	slen := s.Len()
	if slen <= 0 {
		return "", nil, nil
	}
	if buildParam == nil {
		buildParam = GetBuildByDriver(driver)
	}
	var cols []*FieldDB
	// var schema map[string]FieldDB
	if len(options) > 0 && options[0] != nil {
		cols = options[0].Columns
		// schema = options[0].Fields
	} else {
		first := s.Index(0).Interface()
		modelType := reflect.TypeOf(first)
		m := CreateSchema(modelType)
		cols = m.Columns
	}
	placeholders := make([]string, 0)
	args := make([]interface{}, 0)
	if driver != DriverOracle {
		i := 1
		boolSupport := driver == DriverPostgres
		icols := make([]string, 0)
		for _, fdb := range cols {
			if fdb.Insert {
				icols = append(icols, fdb.Column)
			}
		}
		for j := 0; j < slen; j++ {
			model := s.Index(j).Interface()
			mv := reflect.ValueOf(model)
			if mv.Kind() == reflect.Ptr {
				mv = mv.Elem()
			}
			values := make([]string, 0)
			for _, fdb := range cols {
				if fdb.Insert {
					f := mv.Field(fdb.Index)
					fieldValue := f.Interface()
					isNil := false
					if f.Kind() == reflect.Ptr {
						if reflect.ValueOf(fieldValue).IsNil() {
							isNil = true
						} else {
							fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
						}
					}
					if isNil {
						values = append(values, "null")
					} else {
						v, ok := GetDBValue(fieldValue, boolSupport, fdb.Scale)
						if ok {
							values = append(values, v)
						} else {
							if boolValue, ok := fieldValue.(bool); ok {
								if boolValue {
									if fdb.True != nil {
										values = append(values, buildParam(i))
										i = i + 1
										args = append(args, *fdb.True)
									} else {
										values = append(values, "'1'")
									}
								} else {
									if fdb.False != nil {
										values = append(values, buildParam(i))
										i = i + 1
										args = append(args, *fdb.False)
									} else {
										values = append(values, "'0'")
									}
								}
							} else {
								values = append(values, buildParam(i))
								i = i + 1
								if toArray != nil && reflect.TypeOf(fieldValue).Kind() == reflect.Slice {
									args = append(args, toArray(fieldValue))
								} else {
									args = append(args, fieldValue)
								}
							}
						}
					}
				}
			}
			x := "(" + strings.Join(values, ",") + ")"
			placeholders = append(placeholders, x)
		}
		query := fmt.Sprintf(fmt.Sprintf("insert into %s (%s) values %s",
			table,
			strings.Join(icols, ","),
			strings.Join(placeholders, ","),
		))
		return query, args, nil
	} else {
		i := 1
		for j := 0; j < slen; j++ {
			model := s.Index(j).Interface()
			mv := reflect.ValueOf(model)
			iCols := make([]string, 0)
			values := make([]string, 0)
			for _, fdb := range cols {
				if fdb.Insert {
					f := mv.Field(fdb.Index)
					fieldValue := f.Interface()
					isNil := false
					if f.Kind() == reflect.Ptr {
						if reflect.ValueOf(fieldValue).IsNil() {
							isNil = true
						} else {
							fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
						}
					}
					if !isNil {
						iCols = append(iCols, fdb.Column)
						v, ok := GetDBValue(fieldValue, false, fdb.Scale)
						if ok {
							values = append(values, v)
						} else {
							if boolValue, ok := fieldValue.(bool); ok {
								if boolValue {
									if fdb.True != nil {
										values = append(values, buildParam(i))
										i = i + 1
										args = append(args, *fdb.True)
									} else {
										values = append(values, "'1'")
									}
								} else {
									if fdb.False != nil {
										values = append(values, buildParam(i))
										i = i + 1
										args = append(args, *fdb.False)
									} else {
										values = append(values, "'0'")
									}
								}
							} else {
								values = append(values, buildParam(i))
								i = i + 1
								args = append(args, fieldValue)
							}
						}
					}
				}
			}
			x := fmt.Sprintf("into %s(%s)values(%s)", table, strings.Join(iCols, ","), strings.Join(values, ","))
			placeholders = append(placeholders, x)
		}
		query := fmt.Sprintf("insert all %s select * from dual", strings.Join(placeholders, " "))
		return query, args, nil
	}
}
func BuildToSaveBatch(table string, models interface{}, drive string, options ...*Schema) ([]Statement, error) {
	return BuildToSaveBatchWithArray(table, models, drive, nil, options...)
}
func BuildToSaveBatchWithArray(table string, models interface{}, drive string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) ([]Statement, error) {
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models must be a slice")
	}
	slen := s.Len()
	if slen <= 0 {
		return nil, nil
	}
	buildParam := GetBuildByDriver(drive)
	var strt *Schema
	if len(options) > 0 && options[0] != nil {
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
		query, args, err := BuildToSaveWithSchema(table, model, drive, buildParam, toArray, strt)
		if err != nil {
			return stmts, err
		}
		s := Statement{Query: query, Params: args}
		stmts = append(stmts, s)
	}
	return stmts, nil
}
