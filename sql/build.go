package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func BuildToInsert(table string, model interface{}, buildParam func(int) string, options ...*Schema) (string, []interface{}) {
	return BuildToInsertWithSchema(table, model, -1, buildParam, false, false, nil, options...)
}
func BuildToInsertWithBool(table string, model interface{}, buildParam func(int) string, boolSupport bool, options ...*Schema) (string, []interface{}) {
	return BuildToInsertWithSchema(table, model, -1, buildParam, boolSupport, false, nil, options...)
}
func BuildToInsertWithArray(table string, model interface{}, buildParam func(int) string, boolSupport bool, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) (string, []interface{}) {
	return BuildToInsertWithSchema(table, model, -1, buildParam, boolSupport, false, toArray, options...)
}
func BuildToInsertWithVersion(table string, model interface{}, versionIndex int, buildParam func(int) string, boolSupport bool, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) (string, []interface{}) {
	return BuildToInsertWithSchema(table, model, versionIndex, buildParam, boolSupport, false, toArray, options...)
}
func BuildToInsertWithSchema(table string, model interface{}, versionIndex int, buildParam func(int) string, boolSupport bool, includeNull bool, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) (string, []interface{}) {
	modelType := reflect.TypeOf(model)
	var cols []*FieldDB
	if len(options) > 0 && options[0] != nil {
		cols = options[0].Columns
	} else {
		sc := CreateSchema(modelType)
		cols = sc.Columns
	}
	mv := reflect.ValueOf(model)
	if mv.Kind() == reflect.Ptr {
		mv = mv.Elem()
	}
	values := make([]string, 0)
	args := make([]interface{}, 0)
	icols := make([]string, 0)
	i := 1
	for _, fdb := range cols {
		if fdb.Index == versionIndex {
			icols = append(icols, fdb.Column)
			values = append(values, "1")
		} else {
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
			if fdb.Insert {
				if isNil {
					if includeNull {
						icols = append(icols, fdb.Column)
						values = append(values, "null")
					}
				} else {
					icols = append(icols, fdb.Column)
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
	}
	return fmt.Sprintf("insert into %v(%v) values (%v)", table, strings.Join(icols, ","), strings.Join(values, ",")), args
}
func BuildToUpdate(table string, model interface{}, buildParam func(int) string, options ...*Schema) (string, []interface{}) {
	return BuildToUpdateWithVersion(table, model, -1, buildParam, false, nil, options...)
}
func BuildToUpdateWithBool(table string, model interface{}, buildParam func(int) string, boolSupport bool, options ...*Schema) (string, []interface{}) {
	return BuildToUpdateWithVersion(table, model, -1, buildParam, boolSupport, nil, options...)
}
func BuildToUpdateWithArray(table string, model interface{}, buildParam func(int) string, boolSupport bool, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) (string, []interface{}) {
	return BuildToUpdateWithVersion(table, model, -1, buildParam, boolSupport, toArray, options...)
}
func BuildToUpdateWithVersion(table string, model interface{}, versionIndex int, buildParam func(int) string, boolSupport bool, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) (string, []interface{}) {
	var cols, keys []*FieldDB
	// var schema map[string]FieldDB
	modelType := reflect.TypeOf(model)
	if len(options) > 0 && options[0] != nil {
		m := options[0]
		cols = m.Columns
		keys = m.Keys
	} else {
		m := CreateSchema(modelType)
		cols = m.Columns
		keys = m.Keys
	}
	mv := reflect.ValueOf(model)
	if mv.Kind() == reflect.Ptr {
		mv = mv.Elem()
	}
	values := make([]string, 0)
	where := make([]string, 0)
	args := make([]interface{}, 0)
	vw := ""
	i := 1
	for _, fdb := range cols {
		if fdb.Index == versionIndex {
			valueOfModel := reflect.Indirect(reflect.ValueOf(model))
			currentVersion := reflect.Indirect(valueOfModel.Field(versionIndex)).Int()
			nv := currentVersion + 1
			values = append(values, fdb.Column+"="+strconv.FormatInt(nv, 10))
			vw = fdb.Column + "=" + strconv.FormatInt(currentVersion, 10)
		} else if !fdb.Key && fdb.Update {
			//f := reflect.Indirect(reflect.ValueOf(model))
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
				values = append(values, fdb.Column+"=null")
			} else {
				v, ok := GetDBValue(fieldValue, boolSupport, fdb.Scale)
				if ok {
					values = append(values, fdb.Column+"="+v)
				} else {
					if boolValue, ok := fieldValue.(bool); ok {
						if boolValue {
							if fdb.True != nil {
								values = append(values, fdb.Column+"="+buildParam(i))
								i = i + 1
								args = append(args, *fdb.True)
							} else {
								values = append(values, fdb.Column+"='1'")
							}
						} else {
							if fdb.False != nil {
								values = append(values, fdb.Column+"="+buildParam(i))
								i = i + 1
								args = append(args, *fdb.False)
							} else {
								values = append(values, fdb.Column+"='0'")
							}
						}
					} else {
						values = append(values, fdb.Column+"="+buildParam(i))
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
	for _, fdb := range keys {
		f := mv.Field(fdb.Index)
		fieldValue := f.Interface()
		if f.Kind() == reflect.Ptr {
			if !reflect.ValueOf(fieldValue).IsNil() {
				fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
			}
		}
		v, ok := GetDBValue(fieldValue, boolSupport, fdb.Scale)
		if ok {
			where = append(where, fdb.Column+"="+v)
		} else {
			where = append(where, fdb.Column+"="+buildParam(i))
			i = i + 1
			args = append(args, fieldValue)
		}
	}
	if len(vw) > 0 {
		where = append(where, vw)
	}
	query := fmt.Sprintf("update %v set %v where %v", table, strings.Join(values, ","), strings.Join(where, " and "))
	return query, args
}
