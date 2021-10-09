package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"log"
	"reflect"
)

type Writer struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	IdName     string
	idx        int
	modelType  reflect.Type
	modelsType reflect.Type
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewWriterWithId(client *firestore.Client, collectionName string, modelType reflect.Type, fieldName string, options ...func(context.Context, interface{}) (interface{}, error)) *Writer {
	var idx int
	if len(fieldName) == 0 {
		idx, fieldName, _ = FindIdField(modelType)
		if idx < 0 {
			log.Println("Require Id value (Ex Load, Exist, Update, Save) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
		}
	} else {
		idx, _, _ = FindFieldByName(modelType, fieldName)
	}

	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	collection := client.Collection(collectionName)
	return &Writer{client: client, collection: collection, IdName: fieldName, idx: idx, modelType: modelType, modelsType: modelsType, Map: mp}
}

func NewWriter(client *firestore.Client, collectionName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *Writer {
	return NewWriterWithId(client, collectionName, modelType, "", options...)
}

func (w *Writer) Write(ctx context.Context, model interface{}) error {
	id := getIdValueFromModel(model, w.idx)
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		if len(id) == 0 {
			doc := w.collection.NewDoc()
			_, er1 := doc.Create(ctx, m2)
			return er1
		}
		_, er1 := Save(ctx, w.collection, id, m2)
		return er1
	}
	if len(id) == 0 {
		doc := w.collection.NewDoc()
		_, er2 := doc.Create(ctx, model)
		return er2
	}
	_, er2 := Save(ctx, w.collection, id, model)
	return er2
}
