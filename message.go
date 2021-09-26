package mq

import "time"

type Message struct {
	Id         string            `json:"id,omitempty" gorm:"column:id;primary_key" bson:"id,omitempty" dynamodbav:"id,omitempty" firestore:"id,omitempty"`
	Data       []byte            `json:"data,omitempty" gorm:"column:data" bson:"data,omitempty" dynamodbav:"data,omitempty" firestore:"data,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty" gorm:"column:attributes" bson:"attributes,omitempty" dynamodbav:"attributes,omitempty" firestore:"attributes,omitempty"`
	Timestamp  *time.Time        `json:"timestamp,omitempty" gorm:"column:timestamp" bson:"timestamp,omitempty" dynamodbav:"timestamp,omitempty" firestore:"timestamp,omitempty"`
	Raw        interface{}       `json:"-" bson:"-" dynamodbav:"-" firestore:"-"`
	Value      interface{}       `json:"-" bson:"-" dynamodbav:"-" firestore:"-"`
}
