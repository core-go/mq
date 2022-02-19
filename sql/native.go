package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
)

func BuildToSave(table string, model interface{}, driver string, options ...*Schema) (string, []interface{}, error) {
	buildParam := GetBuildByDriver(driver)
	return BuildToSaveWithSchema(table, model, driver, buildParam, nil, options...)
}
func BuildToSaveWithArray(table string, model interface{}, driver string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) (string, []interface{}, error) {
	buildParam := GetBuildByDriver(driver)
	return BuildToSaveWithSchema(table, model, driver, buildParam, toArray, options...)
}
func BuildToSaveWithSchema(table string, model interface{}, driver string, buildParam func(i int) string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) (string, []interface{}, error) {
	// driver := GetDriver(db)
	if buildParam == nil {
		buildParam = GetBuildByDriver(driver)
	}
	modelType := reflect.Indirect(reflect.ValueOf(model)).Type()
	mv := reflect.ValueOf(model)
	if mv.Kind() == reflect.Ptr {
		mv = mv.Elem()
	}
	var cols, keys []*FieldDB
	// var schema map[string]FieldDB
	if len(options) > 0 && options[0] != nil {
		m := options[0]
		cols = m.Columns
		keys = m.Keys
		// schema = m.Fields
	} else {
		// cols, keys, schema = MakeSchema(modelType)
		m := CreateSchema(modelType)
		cols = m.Columns
		keys = m.Keys
		// schema = m.Fields
	}
	if driver == DriverPostgres || driver == DriverMysql {
		iCols := make([]string, 0)
		values := make([]string, 0)
		setColumns := make([]string, 0)
		args := make([]interface{}, 0)
		boolSupport := driver == DriverPostgres
		i := 1
		for _, fdb := range cols {
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
		for _, fdb := range cols {
			if !fdb.Key && fdb.Update {
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
					setColumns = append(setColumns, fdb.Column+"=null")
				} else {
					v, ok := GetDBValue(fieldValue, boolSupport, fdb.Scale)
					if ok {
						setColumns = append(setColumns, fdb.Column+"="+v)
					} else {
						if boolValue, ok := fieldValue.(bool); ok {
							if boolValue {
								if fdb.True != nil {
									setColumns = append(setColumns, fdb.Column+"="+buildParam(i))
									i = i + 1
									args = append(args, *fdb.True)
								} else {
									setColumns = append(setColumns, fdb.Column+"='1'")
								}
							} else {
								if fdb.False != nil {
									setColumns = append(setColumns, fdb.Column+"="+buildParam(i))
									i = i + 1
									args = append(args, *fdb.False)
								} else {
									setColumns = append(setColumns, fdb.Column+"='0'")
								}
							}
						} else {
							setColumns = append(setColumns, fdb.Column+"="+buildParam(i))
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
		var query string
		iKeys := make([]string, 0)
		for _, fdb := range keys {
			iKeys = append(iKeys, fdb.Column)
		}
		if len(setColumns) > 0 {
			if driver == DriverPostgres {
				query = fmt.Sprintf("insert into %s(%s) values (%s) on conflict (%s) do update set %s",
					table,
					strings.Join(iCols, ","),
					strings.Join(values, ","),
					strings.Join(iKeys, ","),
					strings.Join(setColumns, ","),
				)
			} else {
				query = fmt.Sprintf("insert into %s(%s) values (%s) on duplicate key update %s",
					table,
					strings.Join(iCols, ","),
					strings.Join(values, ","),
					strings.Join(setColumns, ","),
				)
			}
		} else {
			if driver == DriverPostgres {
				query = fmt.Sprintf("insert into %s(%s) values (%s) on conflict (%s) do nothing",
					table,
					strings.Join(iCols, ","),
					strings.Join(values, ","),
					strings.Join(iKeys, ","),
				)
			} else {
				query = fmt.Sprintf("insert ignore into %s(%s) values (%s)",
					table,
					strings.Join(iCols, ","),
					strings.Join(values, ","),
				)
			}
		}
		return query, args, nil
	} else if driver == DriverSqlite3 {
		iCols := make([]string, 0)
		values := make([]string, 0)
		args := make([]interface{}, 0)
		i := 1
		for _, fdb := range cols {
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
			iCols = append(iCols, fdb.Column)
			if isNil {
				values = append(values, "null")
			} else {
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
						if toArray != nil && reflect.TypeOf(fieldValue).Kind() == reflect.Slice {
							args = append(args, toArray(fieldValue))
						} else {
							args = append(args, fieldValue)
						}
					}
				}
			}
		}
		query := fmt.Sprintf("insert or replace into %s(%s) values (%s)", table, strings.Join(iCols, ","), strings.Join(values, ","))
		return query, args, nil
	} else {
		dbColumns := make([]string, 0)
		variables := make([]string, 0)
		uniqueCols := make([]string, 0)
		inColumns := make([]string, 0)
		values := make([]interface{}, 0)
		insertCols := make([]string, 0)
		var setColumns []string
		i := 0
		switch driver {
		case DriverOracle:
			for _, fdb := range cols {
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				tkey := `"` + strings.Replace(fdb.Column, `"`, `""`, -1) + `"`
				tkey = strings.ToUpper(tkey)
				inColumns = append(inColumns, "temp."+fdb.Column)
				if fdb.Key {
					onDupe := "a." + tkey + "=" + "temp." + tkey
					uniqueCols = append(uniqueCols, onDupe)
				} else {
					setColumns = append(setColumns, "a."+tkey+" = temp."+tkey)
				}
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				if isNil {
					variables = append(variables, "null "+tkey)
				} else {
					v, ok := GetDBValue(fieldValue, false, fdb.Scale)
					if ok {
						variables = append(variables, v+" "+tkey)
					} else {
						if boolValue, ok := fieldValue.(bool); ok {
							if boolValue {
								if fdb.True != nil {
									variables = append(variables, buildParam(i)+" "+tkey)
									values = append(values, *fdb.True)
									i++
								} else {
									variables = append(variables, "1 "+tkey)
								}
							} else {
								if fdb.False != nil {
									variables = append(variables, buildParam(i)+" "+tkey)
									values = append(values, *fdb.False)
									i++
								} else {
									variables = append(variables, "0 "+tkey)
								}
							}
						} else {
							variables = append(variables, buildParam(i)+" "+tkey)
							i++
							if toArray != nil && reflect.TypeOf(fieldValue).Kind() == reflect.Slice {
								values = append(values, toArray(fieldValue))
							} else {
								values = append(values, fieldValue)
							}
						}
					}
				}
				insertCols = append(insertCols, tkey)
			}

			query := fmt.Sprintf("MERGE INTO %s a USING (SELECT %s FROM dual) temp ON  (%s) WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
				table,
				strings.Join(variables, ", "),
				strings.Join(uniqueCols, " AND "),
				strings.Join(setColumns, ", "),
				strings.Join(insertCols, ", "),
				strings.Join(inColumns, ", "),
			)
			return query, values, nil
		case DriverMssql:
			for _, fdb := range cols {
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				tkey := strings.Replace(fdb.Column, `"`, `""`, -1)
				isNil := false
				if fdb.Key {
					onDupe := table + "." + tkey + "=" + "temp." + tkey
					uniqueCols = append(uniqueCols, onDupe)
				}
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				if isNil {
					variables = append(variables, "null")
				} else {
					v, ok := GetDBValue(fieldValue, false, fdb.Scale)
					if ok {
						variables = append(variables, v)
					} else {
						if boolValue, ok := fieldValue.(bool); ok {
							if boolValue {
								if fdb.True != nil {
									variables = append(variables, "?")
									values = append(values, *fdb.True)
								} else {
									variables = append(variables, "'1' "+tkey)
								}
							} else {
								if fdb.False != nil {
									variables = append(variables, "?")
									values = append(values, *fdb.False)
								} else {
									variables = append(variables, "'0' "+tkey)
								}
							}
						} else {
							variables = append(variables, "?")
							values = append(values, fieldValue)
						}
					}
				}
				dbColumns = append(dbColumns, tkey)
				setColumns = append(setColumns, table+"."+tkey+"=temp."+tkey)
				inColumns = append(inColumns, "temp."+fdb.Column)
			}
			query := fmt.Sprintf("MERGE INTO %s USING (SELECT %s) AS temp (%s) ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
				table,
				strings.Join(variables, ", "),
				strings.Join(dbColumns, ", "),
				strings.Join(uniqueCols, " AND "),
				strings.Join(setColumns, ", "),
				strings.Join(dbColumns, ", "),
				strings.Join(inColumns, ", "),
			)
			return query, values, nil
		default:
			return "", nil, fmt.Errorf("unsupported db vendor")
		}
	}
}
