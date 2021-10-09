package elasticsearch

import (
	"context"
	es "github.com/elastic/go-elasticsearch/v7"
	"reflect"
)

type Inserter struct {
	client    *es.Client
	indexName string
	Map       func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewInserter(client *es.Client, indexName string, options ...func(context.Context, interface{}) (interface{}, error)) *Inserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 {
		mp = options[0]
	}
	return &Inserter{client: client, indexName: indexName, Map: mp}
}

func (w *Inserter) Write(ctx context.Context, model interface{}) error {
	modelType := reflect.TypeOf(model)
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		_, er1 := InsertOne(ctx, w.client, w.indexName, modelType, m2)
		return er1
	}
	_, er2 := InsertOne(ctx, w.client, w.indexName, modelType, model)
	return er2
}
