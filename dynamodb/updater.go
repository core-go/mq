package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"strings"
)

type Updater struct {
	DB        	*dynamodb.DynamoDB
	tableName 	string
	keys   		[]string
	Map    func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewUpdater(database *dynamodb.DynamoDB, tableName string, keys []string, options ...func(context.Context, interface{}) (interface{}, error)) *Updater {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return &Updater{DB: database, tableName: tableName, keys: keys, Map: mp}
}

func (w *Updater) Write(ctx context.Context, model interface{}) error {
	var modelNew interface{}
	var err error
	if w.Map != nil {
		modelNew, err = w.Map(ctx, model)
		if err != nil {
			return err
		}
	} else {
		modelNew = model
	}
	_ ,err = UpdateOne(ctx, w.DB, w.tableName, w.keys, modelNew)
	return err
}

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
