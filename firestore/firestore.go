package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"reflect"
	"strings"
)

func FindIdField(modelType reflect.Type) (int, string, string) {
	return findBsonField(modelType, "_id")
}
func findBsonField(modelType reflect.Type, bsonName string) (int, string, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		bsonTag := field.Tag.Get("bson")
		tags := strings.Split(bsonTag, ",")
		json := field.Name
		if tag1, ok1 := field.Tag.Lookup("json"); ok1 {
			json = strings.Split(tag1, ",")[0]
		}
		for _, tag := range tags {
			if strings.TrimSpace(tag) == bsonName {
				return i, field.Name, json
			}
		}
	}
	return -1, "", ""
}
func FindFieldByName(modelType reflect.Type, fieldName string) (int, string, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		if field.Name == fieldName {
			name1 := fieldName
			name2 := fieldName
			tag1, ok1 := field.Tag.Lookup("json")
			tag2, ok2 := field.Tag.Lookup("firestore")
			if ok1 {
				name1 = strings.Split(tag1, ",")[0]
			}
			if ok2 {
				name2 = strings.Split(tag2, ",")[0]
			}
			return i, name1, name2
		}
	}
	return -1, fieldName, fieldName
}
func getIdValueFromModel(model interface{}, idIndex int) string {
	if id, exist := getFieldValueAtIndex(model, idIndex).(string); exist {
		return id
	}
	return ""
}
func getFieldValueAtIndex(model interface{}, index int) interface{} {
	modelValue := reflect.Indirect(reflect.ValueOf(model))
	return modelValue.Field(index).Interface()
}
func Save(ctx context.Context, collection *firestore.CollectionRef, id string, model interface{}) (int64, error) {
	if len(id) == 0 {
		return 0, fmt.Errorf("cannot update one an object that do not have id field")
	}
	_, err := collection.Doc(id).Set(ctx, model)
	if err != nil {
		return 0, err
	}
	return 1, nil
}
