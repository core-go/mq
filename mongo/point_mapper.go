package mongo

import (
	"context"
	"reflect"
	"strings"
)

type PointMapper struct {
	modelType      reflect.Type
	latitudeIndex  int
	longitudeIndex int
	bsonIndex      int
	latitudeName   string
	longitudeName  string
	bsonName       string
}

//For Get By Id
func findFieldIndex(modelType reflect.Type, fieldName string) int {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		if field.Name == fieldName {
			return i
		}
	}
	return -1
}
func getBsonName(modelType reflect.Type, fieldName string) string {
	field, found := modelType.FieldByName(fieldName)
	if !found {
		return fieldName
	}
	if tag, ok := field.Tag.Lookup("bson"); ok {
		return strings.Split(tag, ",")[0]
	}
	return fieldName
}
func getJsonByIndex(modelType reflect.Type, fieldIndex int) string {
	if tag, ok := modelType.Field(fieldIndex).Tag.Lookup("json"); ok {
		return strings.Split(tag, ",")[0]
	}
	return ""
}
func getBsonNameByIndex(modelType reflect.Type, fieldIndex int) string {
	if tag, ok := modelType.Field(fieldIndex).Tag.Lookup("bson"); ok {
		return strings.Split(tag, ",")[0]
	}
	return ""
}
func FindGeoIndex(modelType reflect.Type) int {
	numField := modelType.NumField()
	k := JSON{}
	for i := 0; i < numField; i++ {
		t := modelType.Field(i).Type
		if t == reflect.TypeOf(&k) || t == reflect.TypeOf(k) {
			return i
		}
	}
	return -1
}

func NewMapper(modelType reflect.Type, options ...string) *PointMapper {
	var bsonName, latitudeName, longitudeName string
	if len(options) >= 1 && len(options[0]) > 0 {
		bsonName = options[0]
	}
	if len(options) >= 2 && len(options[1]) > 0 {
		latitudeName = options[1]
	} else {
		latitudeName = "Latitude"
	}
	if len(options) >= 3 && len(options[2]) > 0 {
		longitudeName = options[2]
	} else {
		longitudeName = "Longitude"
	}
	latitudeIndex := findFieldIndex(modelType, latitudeName)
	longitudeIndex := findFieldIndex(modelType, longitudeName)
	var bsonIndex int
	if len(bsonName) > 0 {
		bsonIndex = findFieldIndex(modelType, bsonName)
	} else {
		bsonIndex = FindGeoIndex(modelType)
	}

	return &PointMapper{
		modelType:      modelType,
		latitudeIndex:  latitudeIndex,
		longitudeIndex: longitudeIndex,
		bsonIndex:      bsonIndex,
		latitudeName:   latitudeName,
		longitudeName:  longitudeName,
		bsonName:       bsonName,
	}
}

func (s *PointMapper) DbToModel(ctx context.Context, model interface{}) (interface{}, error) {
	valueModelObject := reflect.Indirect(reflect.ValueOf(model))
	if valueModelObject.Kind() == reflect.Ptr {
		valueModelObject = reflect.Indirect(valueModelObject)
	}
	k := valueModelObject.Kind()
	if k == reflect.Map || k == reflect.Struct {
		s.bsonToPoint(valueModelObject, s.bsonIndex, s.latitudeIndex, s.longitudeIndex)
	}
	return model, nil
}

func (s *PointMapper) DbToModels(ctx context.Context, model interface{}) (interface{}, error) {
	vo := reflect.Indirect(reflect.ValueOf(model))
	if vo.Kind() == reflect.Ptr {
		vo = reflect.Indirect(vo)
	}

	if vo.Kind() == reflect.Slice {
		for i := 0; i < vo.Len(); i++ {
			s.bsonToPoint(vo.Index(i), s.bsonIndex, s.latitudeIndex, s.longitudeIndex)
		}
	}
	return model, nil
}

func (s *PointMapper) ModelToDb(ctx context.Context, model interface{}) (interface{}, error) {
	m, ok := model.(map[string]interface{})
	if ok {
		latJson := getJsonByIndex(s.modelType, s.latitudeIndex)
		logJson := getJsonByIndex(s.modelType, s.longitudeIndex)
		bs := getBsonNameByIndex(s.modelType, s.bsonIndex)
		m2 := FromPointMap(m, bs, latJson, logJson)
		return m2, nil
	}
	vo := reflect.Indirect(reflect.ValueOf(model))
	k := vo.Kind()
	if k == reflect.Ptr {
		vo = reflect.Indirect(vo)
	}
	if k == reflect.Struct {
		FromPoint(vo, s.bsonIndex, s.latitudeIndex, s.longitudeIndex)
	}
	return model, nil
}
func (s *PointMapper) ModelsToDb(ctx context.Context, model interface{}) (interface{}, error) {
	vo := reflect.Indirect(reflect.ValueOf(model))
	if vo.Kind() == reflect.Ptr {
		vo = reflect.Indirect(vo)
	}

	if vo.Kind() == reflect.Slice {
		for i := 0; i < vo.Len(); i++ {
			FromPoint(vo.Index(i), s.bsonIndex, s.latitudeIndex, s.longitudeIndex)
		}
	}
	return model, nil
}

func ToPoint(value reflect.Value, bsonIndex int, latitudeIndex int, longitudeIndex int) {
	if value.Kind() == reflect.Struct {
		x := reflect.Indirect(value)
		b := x.Field(bsonIndex)
		k := b.Kind()
		if k == reflect.Struct || (k == reflect.Ptr && b.IsNil() == false) {
			arrLatLong := reflect.Indirect(b).FieldByName("GeoJSON").Interface()
			latitude := reflect.Indirect(reflect.ValueOf(arrLatLong)).Index(0).Interface()
			longitude := reflect.Indirect(reflect.ValueOf(arrLatLong)).Index(1).Interface()

			latField := x.Field(latitudeIndex)
			if latField.Kind() == reflect.Ptr {
				var f *float64
				var f2 = latitude.(float64)
				f = &f2
				latField.Set(reflect.ValueOf(f))
			} else {
				latField.Set(reflect.ValueOf(latitude))
			}
			lonField := x.Field(longitudeIndex)
			if lonField.Kind() == reflect.Ptr {
				var f *float64
				var f2 = latitude.(float64)
				f = &f2
				lonField.Set(reflect.ValueOf(f))
			} else {
				lonField.Set(reflect.ValueOf(longitude))
			}
		}
	}
}
func (s *PointMapper) bsonToPoint(value reflect.Value, bsonIndex int, latitudeIndex int, longitudeIndex int) {
	if value.Kind() == reflect.Struct {
		x := reflect.Indirect(value)
		b := x.Field(bsonIndex)
		k := b.Kind()
		if k == reflect.Struct || (k == reflect.Ptr && b.IsNil() == false) {
			arrLatLong := reflect.Indirect(b).FieldByName("Coordinates").Interface()
			latitude := reflect.Indirect(reflect.ValueOf(arrLatLong)).Index(0).Interface()
			longitude := reflect.Indirect(reflect.ValueOf(arrLatLong)).Index(1).Interface()

			latField := x.Field(latitudeIndex)
			if latField.Kind() == reflect.Ptr {
				var f *float64
				var f2 = latitude.(float64)
				f = &f2
				latField.Set(reflect.ValueOf(f))
			} else {
				latField.Set(reflect.ValueOf(latitude))
			}
			lonField := x.Field(longitudeIndex)
			if lonField.Kind() == reflect.Ptr {
				var f *float64
				var f2 = longitude.(float64)
				f = &f2
				lonField.Set(reflect.ValueOf(f))
			} else {
				lonField.Set(reflect.ValueOf(longitude))
			}
		}
	}

	if value.Kind() == reflect.Map {
		var arrLatLongTag, latitudeTag, longitudeTag string
		if arrLatLongTag = getBsonName(s.modelType, s.bsonName); arrLatLongTag == "" || arrLatLongTag == "-" {
			arrLatLongTag = getJsonName(s.modelType, s.bsonName)
		}

		if latitudeTag = getBsonName(s.modelType, s.latitudeName); latitudeTag == "" || latitudeTag == "-" {
			latitudeTag = getJsonName(s.modelType, s.latitudeName)
		}

		if longitudeTag = getBsonName(s.modelType, s.longitudeName); longitudeTag == "" || longitudeTag == "-" {
			longitudeTag = getJsonName(s.modelType, s.longitudeName)
		}

		arrLatLong := reflect.Indirect(reflect.ValueOf(value.MapIndex(reflect.ValueOf(arrLatLongTag)).Interface())).MapIndex(reflect.ValueOf("coordinates")).Interface()
		latitude := reflect.Indirect(reflect.ValueOf(arrLatLong)).Index(0).Interface()
		longitude := reflect.Indirect(reflect.ValueOf(arrLatLong)).Index(1).Interface()

		value.SetMapIndex(reflect.ValueOf(latitudeTag), reflect.ValueOf(latitude))
		value.SetMapIndex(reflect.ValueOf(longitudeTag), reflect.ValueOf(longitude))

		//delete field
		value.SetMapIndex(reflect.ValueOf(arrLatLongTag), reflect.Value{})
	}
}
func FromPointMap(m map[string]interface{}, bsonName string, latitudeJson string, longitudeJson string) map[string]interface{} {
	latV, ok1 := m[latitudeJson]
	logV, ok2 := m[longitudeJson]
	if ok1 && ok2 && len(bsonName) > 0 {
		la, ok3 := latV.(float64)
		lo, ok4 := logV.(float64)
		if ok3 && ok4 {
			var arr []float64
			arr = append(arr, la, lo)
			ml := JSON{Type: "Point", Coordinates: arr}
			m2 := make(map[string]interface{})
			m2[bsonName] = ml
			for key := range m {
				if key != latitudeJson && key != longitudeJson {
					m2[key] = m[key]
				}
			}
			return m2
		}
	}
	return m
}
func FromPoint(value reflect.Value, bsonIndex int, latitudeIndex int, longitudeIndex int) {
	v := reflect.Indirect(value)
	latitudeField := v.Field(latitudeIndex)
	latNil := false
	if latitudeField.Kind() == reflect.Ptr {
		if latitudeField.IsNil() {
			latNil = true
		}
		latitudeField = reflect.Indirect(latitudeField)
	}
	longNil := false
	longitudeField := v.Field(longitudeIndex)
	if longitudeField.Kind() == reflect.Ptr {
		if longitudeField.IsNil() {
			longNil = true
		}
		longitudeField = reflect.Indirect(longitudeField)
	}
	if latNil == false && longNil == false {
		latitude := latitudeField.Interface()
		longitude := longitudeField.Interface()
		la, ok3 := latitude.(float64)
		lo, ok4 := longitude.(float64)
		if ok3 && ok4 {
			var arr []float64
			arr = append(arr, la, lo)
			coordinatesField := v.Field(bsonIndex)
			if coordinatesField.Kind() == reflect.Ptr {
				m := &JSON{Type: "Point", Coordinates: arr}
				coordinatesField.Set(reflect.ValueOf(m))
			} else {
				x := coordinatesField.FieldByName("Type")
				x.Set(reflect.ValueOf("Point"))
				y := coordinatesField.FieldByName("Geo")
				y.Set(reflect.ValueOf(arr))
			}
		}
	}
}

func getJsonName(modelType reflect.Type, fieldName string) string {
	field, found := modelType.FieldByName(fieldName)
	if !found {
		return fieldName
	}
	if tag, ok := field.Tag.Lookup("json"); ok {
		return strings.Split(tag, ",")[0]
	}
	return fieldName
}
