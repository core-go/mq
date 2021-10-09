package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Inserter struct {
	DB        	*dynamodb.DynamoDB
	tableName 	string
	keys   		[]string
	Map       	func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewInserter(database *dynamodb.DynamoDB, tableName string, keys []string, options ...func(context.Context, interface{}) (interface{}, error)) *Inserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return &Inserter{DB: database, tableName: tableName, keys: keys, Map: mp}
}

func (w *Inserter) Write(ctx context.Context, model interface{}) error {
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
	_ ,err = InsertOne(ctx, w.DB, w.tableName, w.keys, modelNew)
	return err
}
