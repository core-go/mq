package dynamodb

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"reflect"
	"strconv"
	"strings"
)

type (
	SecondaryIndex struct {
		IndexName string
		Keys      []string
	}
	Config struct {
		Region          string `mapstructure:"region" json:"region,omitempty" gorm:"column:region" bson:"region,omitempty" dynamodbav:"region,omitempty" firestore:"region,omitempty"`
		AccessKeyID     string `mapstructure:"access_key_id" json:"accessKeyID,omitempty" gorm:"column:accessKeyID" bson:"accessKeyID,omitempty" dynamodbav:"accessKeyID,omitempty" firestore:"accessKeyID,omitempty"`
		SecretAccessKey string `mapstructure:"secret_access_key" json:"secretAccessKey,omitempty" gorm:"column:secretaccesskey" bson:"secretAccessKey,omitempty" dynamodbav:"secretAccessKey,omitempty" firestore:"secretAccessKey,omitempty"`
		Token           string `mapstructure:"token" json:"token,omitempty" gorm:"column:token" bson:"token,omitempty" dynamodbav:"token,omitempty" firestore:"token,omitempty"`
	}
)

func NewSession(config Config) (*session.Session, error) {
	c := &aws.Config{
		Region:      aws.String(config.Region),
		Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.SecretAccessKey, config.Token),
	}
	return session.NewSession(c)
}
func Connect(config Config) (*dynamodb.DynamoDB, error) {
	sess, err := NewSession(config)
	if err != nil {
		return nil, err
	}
	db := dynamodb.New(sess)
	return db, nil
}
func ConnectWithSession(session *session.Session) *dynamodb.DynamoDB {
	return dynamodb.New(session)
}

func Exist(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, id interface{}) (bool, error) {
	keyMap, err := buildKeyMap(keys, id)
	if err != nil {
		return false, err
	}
	input := &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       keyMap,
	}
	resp, err := db.GetItemWithContext(ctx, input)
	if err != nil {
		return false, err
	}
	if len(resp.Item) == 0 {
		return false, nil
	}
	return true, nil
}
func InsertOne(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, model interface{}) (int64, error) {
	var strWhere string
	if len(keys) > 0 {
		strWhere = fmt.Sprintf("attribute_not_exists(%s)", strings.Join(keys, ","))
	}
	modelMap, err := dynamodbattribute.MarshalMap(model)
	if err != nil {
		return 0, err
	}
	params := &dynamodb.PutItemInput{
		TableName:              aws.String(tableName),
		Item:                   modelMap,
		ConditionExpression: 	aws.String(strWhere),
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	output, err := db.PutItemWithContext(ctx, params)
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException:") >= 0 {
			return 0, fmt.Errorf("object exist")
		}
		return 0, err
	}
	return int64(aws.Float64Value(output.ConsumedCapacity.CapacityUnits)), nil
}

func UpdateOne(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, model interface{}) (int64, error) {
	ids := getIdValueFromModel(model, keys)
	expected, err := buildKeyMapWithExpected(keys, ids, true)
	if err != nil {
		return 0, err
	}
	modelMap, err := dynamodbattribute.MarshalMap(model)
	if err != nil {
		return 0, err
	}
	params := &dynamodb.PutItemInput{
		TableName:              aws.String(tableName),
		Expected:               expected,
		Item:                   modelMap,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	output, err := db.PutItemWithContext(ctx, params)
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException:") >= 0 {
			return 0, fmt.Errorf("object not found")
		}
		return 0, err
	}
	return int64(aws.Float64Value(output.ConsumedCapacity.CapacityUnits)), nil
}

func UpsertOne(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, model interface{}) (int64, error) {
	modelMap, err := dynamodbattribute.MarshalMap(model)
	if err != nil {
		return 0, err
	}
	var strWhere string
	if len(keys) > 0 {
		strWhere = fmt.Sprintf("attribute_exists(%s)", strings.Join(keys, ","))
	}
	params := &dynamodb.PutItemInput{
		TableName:              aws.String(tableName),
		Item:                   modelMap,
		ConditionExpression: 	aws.String(strWhere),
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	output, err := db.PutItemWithContext(ctx, params)
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException:") >= 0 {
			return 0, fmt.Errorf("object not found")
		}
		return 0, err
	}
	return int64(aws.Float64Value(output.ConsumedCapacity.CapacityUnits)), nil
}

func DeleteOne(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, id interface{}) (int64, error) {
	keyMap, err := buildKeyMap(keys, id)
	if err != nil {
		return 0, err
	}
	params := &dynamodb.DeleteItemInput{
		TableName:              aws.String(tableName),
		Key:                    keyMap,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	output, err := db.DeleteItemWithContext(ctx, params)
	if err != nil {
		return 0, err
	}
	return int64(aws.Float64Value(output.ConsumedCapacity.CapacityUnits)), nil
}

func PatchOne(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, model map[string]interface{}) (int64, error) {
	idMap := map[string]interface{}{}
	for i := range keys {
		idMap[keys[i]] = model[keys[i]]
		delete(model, keys[i])
	}
	keyMap, err := buildKeyMap(keys, idMap)
	if err != nil {
		return 0, err
	}
	updateBuilder := expression.UpdateBuilder{}
	for key, value := range model {
		updateBuilder = updateBuilder.Set(expression.Name(key), expression.Value(value))
	}
	var cond expression.ConditionBuilder
	for key, value := range idMap {
		if reflect.ValueOf(cond).IsZero() {
			cond = expression.Name(key).Equal(expression.Value(value))
		}
		cond = cond.And(expression.Name(key).Equal(expression.Value(value)))
	}
	expr, _ := expression.NewBuilder().WithUpdate(updateBuilder).WithCondition(cond).Build()
	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(tableName),
		Key:                       keyMap,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	output, err := db.UpdateItemWithContext(ctx, input)
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException:") >= 0 {
			return 0, fmt.Errorf("object not found")
		}
		return 0, err
	}
	return int64(aws.Float64Value(output.ConsumedCapacity.CapacityUnits)), nil
}

func GetFieldByName(modelType reflect.Type, fieldName string) (int, string, bool) {
	numField := modelType.NumField()
	for index := 0; index < numField; index++ {
		field := modelType.Field(index)
		if field.Name == fieldName {
			if dynamodbTag, ok := field.Tag.Lookup("dynamodbav"); ok {
				name := strings.Split(dynamodbTag, ",")[0]
				return index, name, true
			}
			if jsonTag, ok := field.Tag.Lookup("json"); ok {
				name := strings.Split(jsonTag, ",")[0]
				return index, name, true
			}
		}
	}
	return -1, fieldName, false
}

func GetFieldByTagName(modelType reflect.Type, tagName string) (int, string, bool) {
	numField := modelType.NumField()
	for index := 0; index < numField; index++ {
		field := modelType.Field(index)
		if dbTag, ok := field.Tag.Lookup("dynamodbav"); ok && strings.Split(dbTag, ",")[0] == tagName {
			return index, field.Name, true
		}
		if jsonTag, ok := field.Tag.Lookup("json"); ok && strings.Split(jsonTag, ",")[0] == tagName {
			return index, field.Name, true
		}
	}
	return -1, tagName, false
}

func getIdValueFromModel(model interface{}, keys []string) []interface{} {
	var values []interface{}
	modelValue := reflect.Indirect(reflect.ValueOf(model))
	for idx := range keys {
		if index, _, ok := GetFieldByTagName(modelValue.Type(), keys[idx]); ok {
			idValue := modelValue.Field(index).Interface()
			values = append(values, idValue)
		}
	}
	return values
}

func getFieldValueAtIndex(model interface{}, index int) interface{} {
	modelValue := reflect.Indirect(reflect.ValueOf(model))
	return modelValue.Field(index).Interface()
}

func setValueWithIndex(model interface{}, index int, value interface{}) (interface{}, error) {
	valueObject := reflect.Indirect(reflect.ValueOf(model))
	numField := valueObject.NumField()
	if index >= 0 && index < numField {
		valueObject.Field(index).Set(reflect.ValueOf(value))
		return model, nil
	}
	return nil, fmt.Errorf("error no found field index: %v", index)
}

func buildKeyMap(keys []string, value interface{}) (map[string]*dynamodb.AttributeValue, error) {
	idValue := reflect.ValueOf(value)
	idMap := map[string]interface{}{}
	switch idValue.Kind() {
	case reflect.Map:
		for _, key := range keys {
			if !idValue.MapIndex(reflect.ValueOf(key)).IsValid() {
				return nil, fmt.Errorf("wrong mapping key and value")
			}
			idMap[key] = idValue.MapIndex(reflect.ValueOf(key)).Interface()
		}
		if len(idMap) != idValue.Len() {
			return nil, fmt.Errorf("wrong mapping key and value")
		}
	case reflect.Slice, reflect.Array:
		if len(keys) != idValue.Len() {
			return nil, fmt.Errorf("wrong mapping key and value")
		}
		for idx := range keys {
			idMap[keys[idx]] = idValue.Index(idx).Interface()
		}
	default:
		idMap[keys[0]] = idValue.Interface()
	}
	keyMap := map[string]*dynamodb.AttributeValue{}
	for key, value := range idMap {
		v := reflect.ValueOf(value)
		switch v.Kind() {
		case reflect.String:
			keyMap[key] = &dynamodb.AttributeValue{S: aws.String(v.String())}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			keyMap[key] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(v.Int(), 10))}
		case reflect.Float32, reflect.Float64:
			keyMap[key] = &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%g", v.Float()))}
		default:
			return keyMap, fmt.Errorf("data type not support")
		}
	}
	return keyMap, nil
}

func buildKeyMapWithExpected(keys []string, values []interface{}, isExist bool) (map[string]*dynamodb.ExpectedAttributeValue, error) {
	//values := getIdValueFromModel(model, keys)
	if len(values) == 0 {
		return nil, fmt.Errorf("cannot update one an Object that do not have ids field")
	}
	keyMap, err := buildKeyMap(keys, values)
	if err != nil {
		return nil, err
	}
	result := map[string]*dynamodb.ExpectedAttributeValue{}
	for k, v := range keyMap {
		result[k] = &dynamodb.ExpectedAttributeValue{
			Value:  v,
			Exists: aws.Bool(isExist),
		}
	}
	return result, nil
}
