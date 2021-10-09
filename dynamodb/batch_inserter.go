package dynamodb

import (
	"context"
	_ "github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"log"
	"reflect"
)

type BatchInserter struct {
	DB        *dynamodb.DynamoDB
	tableName string
	Map       func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewBatchInserter(database *dynamodb.DynamoDB, tableName string, options ...func(context.Context, interface{}) (interface{}, error)) *BatchInserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return &BatchInserter{DB: database, tableName: tableName, Map: mp}
}

func (w *BatchInserter) Write(ctx context.Context, models interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)
	s := reflect.ValueOf(models)

	var er1 error
	if w.Map != nil {
		m2, er0 := w.Map(ctx, models)
		if er0 != nil {
			return successIndices, failIndices, er0
		}
		_, _, er1 = InsertManySkipErrors(ctx, w.DB, w.tableName, m2)
	} else {
		_, _, er1 = InsertManySkipErrors(ctx, w.DB, w.tableName, models)
	}
	if er1 == nil {
		for i := 0; i < s.Len(); i++ {
			successIndices = append(successIndices, i)
		}
		return successIndices, failIndices, er1
	}else {
		for i := 0; i < s.Len(); i++ {
			failIndices = append(failIndices, i)
		}
	}
	return successIndices, failIndices, er1
}

func BatchWriterItem(ctx context.Context, db *dynamodb.DynamoDB, data []interface{}, tableName string) (*dynamodb.BatchWriteItemOutput, error) {
	listWriteRequest := make([]*dynamodb.WriteRequest, 0)
	for _, d := range data {
		av, _ := dynamodbattribute.MarshalMap(d)
		putRequest := &dynamodb.PutRequest{
			Item: av,
		}
		writeRequest := &dynamodb.WriteRequest{
			DeleteRequest: nil,
			PutRequest:    putRequest,
		}
		listWriteRequest = append(listWriteRequest, writeRequest)
	}

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			tableName: listWriteRequest,
		},
	}
	err := input.Validate()
	if err != nil {
		return nil, err
	}

	rs, err := db.BatchWriteItem(input)
	if err != nil {
		return nil, err
	}
	return rs, nil

}

func BatchWriterItemRequest (ctx context.Context, db *dynamodb.DynamoDB,request map[string][]*dynamodb.WriteRequest, tableName string) (*dynamodb.BatchWriteItemOutput, error){
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: request,
	}
	err := input.Validate()
	if err != nil {
		return nil, err
	}

	rs, err := db.BatchWriteItem(input)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func BatchWriter25(ctx context.Context, db *dynamodb.DynamoDB, data []interface{}, tableName string) ([]*dynamodb.BatchWriteItemOutput, error) {
	lenData := len(data)
	n := lenData / 25
	r := lenData % 25
	var batchResponse = make([]*dynamodb.BatchWriteItemOutput,0)
	var start, end int
	for i := 0; i < n; i++ {
		start = i * 25
		end = start + 25
		d := data[start:end]
		rs, err := BatchWriterItem(ctx, db, d, tableName)
		if err != nil {
			return nil, err
		}
		batchResponse = append(batchResponse,rs)
	}
	start = n * 25
	end = start + r
	d := data[start:end]
	rs, err := BatchWriterItem(ctx, db, d, tableName)
	if err != nil {
		return nil, err
	}
	batchResponse = append(batchResponse,rs)
	return batchResponse, nil

}

func InsertManySkipErrors(ctx context.Context, db *dynamodb.DynamoDB, tableName string, models interface{}) (interface{}, interface{}, error) {
	arr := make([]interface{}, 0)
	modelsType := reflect.TypeOf(models)
	insertedFails := reflect.New(modelsType).Interface()
	switch reflect.TypeOf(models).Kind() {
	case reflect.Slice:
		values := reflect.ValueOf(models)
		if values.Len() == 0 {
			return insertedFails, insertedFails, nil
		}
		for i := 0; i < values.Len(); i++ {
			arr = append(arr, values.Index(i).Interface())
		}
	}

	batchResponses, err := BatchWriterItem(ctx,db,arr,tableName)
	if err != nil {
		return nil, nil, err
	}
	log.Println(batchResponses)
	return nil, nil, err
}

func appendToArray(arr interface{}, item interface{}) interface{} {
	arrValue := reflect.ValueOf(arr)
	elemValue := reflect.Indirect(arrValue)

	itemValue := reflect.ValueOf(item)
	if itemValue.Kind() == reflect.Ptr {
		itemValue = reflect.Indirect(itemValue)
	}
	elemValue.Set(reflect.Append(elemValue, itemValue))
	return arr
}

func existInArray(arr []int64, value interface{}) bool {
	for _, v := range arr {
		if v == value {
			return true
		}
	}
	return false
}

func mapIdInObjects(models interface{}, arrayFailIndexIgnore []int64, insertedIDs []interface{}, modelsType reflect.Type, fieldName string) interface{} {
	insertedSuccess := reflect.New(modelsType).Interface()
	switch reflect.TypeOf(models).Kind() {
	case reflect.Slice:
		values := reflect.ValueOf(models)
		length := values.Len()
		if length > 0 && length == len(insertedIDs) {
			if index, _, _ := GetFieldByName(reflect.TypeOf(models), fieldName); index != -1 {
				for i := 0; i < length; i++ {
					if !existInArray(arrayFailIndexIgnore, i) {
						if id, ok := insertedIDs[i].(*interface{}); ok {
							itemValue := values.Index(i)
							var errSet error
							var vSet interface{}
							switch reflect.Indirect(itemValue).FieldByName(fieldName).Kind() {
							case reflect.String:
								idString := id
								vSet, errSet = setValueWithIndex(itemValue, index, idString)
								break
							default:
								vSet, errSet = setValueWithIndex(itemValue, index, id)
								break
							}
							if errSet == nil {
								appendToArray(insertedSuccess, vSet)
							} else {
								appendToArray(insertedSuccess, itemValue.Interface())
								log.Println("Error map Id: ", errSet)
							}
						}
					}
				}
			}
		}
	}
	return insertedSuccess
}
