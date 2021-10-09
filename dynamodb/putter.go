package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Putter struct {
	database   	*dynamodb.DynamoDB
	tableName  	string
	keys 		[]string
	Map        	func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewPutter(database *dynamodb.DynamoDB, tableName string, keys []string, options ...func(context.Context, interface{}) (interface{}, error)) *Putter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return &Putter{tableName: tableName, database: database, keys: keys, Map: mp}
}

func (w *Putter) Write(ctx context.Context, model interface{}) error {
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
	_, err = UpsertOne(ctx, w.database, w.tableName, w.keys, modelNew)
	return err
}
