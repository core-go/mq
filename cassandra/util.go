package cassandra

import (
	"reflect"
	"strconv"
	"strings"
)

type FieldDB struct {
	JSON   string
	Column string
	Field  string
	Index  int
	Key    bool
	Update bool
	Insert bool
}
type Schema struct {
	SKeys    []string
	SColumns []string
	Keys     []FieldDB
	Columns  []FieldDB
	Fields   map[string]FieldDB
}

func CreateSchema(modelType reflect.Type) *Schema {
	m := modelType
	if m.Kind() == reflect.Ptr {
		m = m.Elem()
	}
	numField := m.NumField()
	scolumns := make([]string, 0)
	skeys := make([]string, 0)
	columns := make([]FieldDB, 0)
	keys := make([]FieldDB, 0)
	schema := make(map[string]FieldDB, 0)
	for idx := 0; idx < numField; idx++ {
		field := m.Field(idx)
		tag, _ := field.Tag.Lookup("gorm")
		if !strings.Contains(tag, IgnoreReadWrite) {
			update := !strings.Contains(tag, "update:false")
			insert := !strings.Contains(tag, "insert:false")
			if has := strings.Contains(tag, "column"); has {
				json := field.Name
				col := json
				str1 := strings.Split(tag, ";")
				num := len(str1)
				for i := 0; i < num; i++ {
					str2 := strings.Split(str1[i], ":")
					for j := 0; j < len(str2); j++ {
						if str2[j] == "column" {
							isKey := strings.Contains(tag, "primary_key")
							col = str2[j+1]
							scolumns = append(scolumns, col)
							jTag, jOk := field.Tag.Lookup("json")
							if jOk {
								tagJsons := strings.Split(jTag, ",")
								json = tagJsons[0]
							}
							f := FieldDB{
								JSON:   json,
								Column: col,
								Index:  idx,
								Key:    isKey,
								Update: update,
								Insert: insert,
							}
							if isKey {
								skeys = append(skeys, col)
								keys = append(keys, f)
							}
							columns = append(columns, f)
							schema[col] = f
						}
					}
				}
			}
		}
	}
	s := &Schema{SColumns: scolumns, SKeys: skeys, Columns: columns, Keys: keys, Fields: schema}
	return s
}
func MakeSchema(modelType reflect.Type) ([]FieldDB, []FieldDB) {
	m := CreateSchema(modelType)
	return m.Columns, m.Keys
}
func GetDBValue(v interface{}) (string, bool) {
	switch v.(type) {
	case string:
		s0 := v.(string)
		if len(s0) == 0 {
			return "''", true
		}
		return "", false
	case bool:
		b0 := v.(bool)
		if b0 {
			return "true", true
		} else {
			return "false", true
		}
	case int:
		return strconv.Itoa(v.(int)), true
	case int64:
		return strconv.FormatInt(v.(int64), 10), true
	case int32:
		return strconv.FormatInt(int64(v.(int32)), 10), true
	default:
		return "", false
	}
}
