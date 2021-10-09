package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch/v7"
	"reflect"
)

type Writer struct {
	client    *elasticsearch.Client
	indexName string
	Map       func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewWriter(client *elasticsearch.Client, indexName string, options ...func(context.Context, interface{}) (interface{}, error)) *Writer {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 {
		mp = options[0]
	}
	return &Writer{client: client, indexName: indexName, Map: mp}
}

func (w *Writer) Write(ctx context.Context, model interface{}) error {
	modelType := reflect.TypeOf(model)
	_, _, id := FindValueByJson(modelType, "id")
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		_, er1 := UpsertOne(ctx, w.client, w.indexName, id, m2)
		return er1
	}
	_, er2 := UpsertOne(ctx, w.client, w.indexName, id, model)
	return er2
}
