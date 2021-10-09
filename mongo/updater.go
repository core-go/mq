package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"reflect"
)

type Updater struct {
	collection *mongo.Collection
	IdName     string
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
	modelType  reflect.Type
}

func NewUpdaterWithId(database *mongo.Database, collectionName string, modelType reflect.Type, fieldName string, options ...func(context.Context, interface{}) (interface{}, error)) *Updater {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	if len(fieldName) == 0 {
		_, idName, _ := FindIdField(modelType)
		fieldName = idName
	}
	collection := database.Collection(collectionName)
	return &Updater{collection: collection, IdName: fieldName, Map: mp, modelType: modelType}
}

func NewUpdater(database *mongo.Database, collectionName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *Updater {
	return NewUpdaterWithId(database, collectionName, modelType, "", options...)
}

func (w *Updater) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		return Update(ctx, w.collection, m2, w.IdName)
	}
	return Update(ctx, w.collection, model, w.IdName)
}
