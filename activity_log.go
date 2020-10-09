package mq

import (
	"context"
	"encoding/json"
	"time"
)

type IdGenerator interface {
	Generate(ctx context.Context) (string, error)
}

func NewActivityLogSender(producer Producer, config ActivityLogConfig, schema ActivityLogSchema, generator IdGenerator, header *[]string) *ActivityLogSender {
	if len(schema.User) == 0 {
		schema.User = "user"
	}
	if len(schema.Resource) == 0 {
		schema.Resource = "resource"
	}
	if len(schema.Action) == 0 {
		schema.Action = "action"
	}
	if len(schema.Timestamp) == 0 {
		schema.Timestamp = "timestamp"
	}
	if len(schema.Status) == 0 {
		schema.Status = "status"
	}
	sender := ActivityLogSender{Producer: producer, Config: config, Schema: schema, Generator: generator}
	return &sender
}

type ActivityLogConfig struct {
	User       string `mapstructure:"user" json:"user,omitempty" gorm:"column:user" bson:"user,omitempty" dynamodbav:"user,omitempty" firestore:"user,omitempty"`
	Ip         string `mapstructure:"ip" json:"ip,omitempty" gorm:"column:ip" bson:"ip,omitempty" dynamodbav:"ip,omitempty" firestore:"ip,omitempty"`
	True       string `mapstructure:"true" json:"true,omitempty" gorm:"column:true" bson:"true,omitempty" dynamodbav:"true,omitempty" firestore:"true,omitempty"`
	False      string `mapstructure:"false" json:"false,omitempty" gorm:"column:false" bson:"false,omitempty" dynamodbav:"false,omitempty" firestore:"false,omitempty"`
	Goroutines bool   `mapstructure:"goroutines" json:"goroutines,omitempty" gorm:"column:goroutines" bson:"goroutines,omitempty" dynamodbav:"goroutines,omitempty" firestore:"goroutines,omitempty"`
}
type ActivityLogSchema struct {
	Id        string    `mapstructure:"id" json:"id,omitempty" gorm:"column:id" bson:"_id,omitempty" dynamodbav:"id,omitempty" firestore:"id,omitempty"`
	User      string    `mapstructure:"user" json:"user,omitempty" gorm:"column:user" bson:"user,omitempty" dynamodbav:"user,omitempty" firestore:"user,omitempty"`
	Ip        string    `mapstructure:"ip" json:"ip,omitempty" gorm:"column:ip" bson:"ip,omitempty" dynamodbav:"ip,omitempty" firestore:"ip,omitempty"`
	Resource  string    `mapstructure:"resource" json:"resource,omitempty" gorm:"column:resource" bson:"resource,omitempty" dynamodbav:"resource,omitempty" firestore:"resource,omitempty"`
	Action    string    `mapstructure:"action" json:"action,omitempty" gorm:"column:action" bson:"action,omitempty" dynamodbav:"action,omitempty" firestore:"action,omitempty"`
	Timestamp string    `mapstructure:"timestamp" json:"timestamp,omitempty" gorm:"column:timestamp" bson:"timestamp,omitempty" dynamodbav:"timestamp,omitempty" firestore:"timestamp,omitempty"`
	Status    string    `mapstructure:"status" json:"status,omitempty" gorm:"column:status" bson:"status,omitempty" dynamodbav:"status,omitempty" firestore:"status,omitempty"`
	Desc      string    `mapstructure:"desc" json:"desc,omitempty" gorm:"column:desc" bson:"desc,omitempty" dynamodbav:"desc,omitempty" firestore:"desc,omitempty"`
	Ext       *[]string `mapstructure:"ext" json:"ext,omitempty" gorm:"column:ext" bson:"ext,omitempty" dynamodbav:"ext,omitempty" firestore:"ext,omitempty"`
	Headers   *[]string `mapstructure:"headers" json:"headers,omitempty" gorm:"column:headers" bson:"headers,omitempty" dynamodbav:"headers,omitempty" firestore:"headers,omitempty"`
}
type ActivityLogSender struct {
	Producer  Producer
	Config    ActivityLogConfig
	Schema    ActivityLogSchema
	Generator IdGenerator
}
func (s *ActivityLogSender) SaveLog(ctx context.Context, resource string, action string, success bool, desc string) error {
	log := make(map[string]interface{})
	ch := s.Schema
	log[ch.Timestamp] = time.Now()
	log[ch.Resource] = resource
	log[ch.Action] = action
	if len(ch.Desc) > 0 {
		log[ch.Desc] = desc
	}
	if success {
		log[ch.Status] = s.Config.True
	} else {
		log[ch.Status] = s.Config.False
	}
	log[ch.User] = GetString(ctx, s.Config.User)
	if len(ch.Ip) > 0 {
		log[ch.Ip] = GetString(ctx, s.Config.Ip)
	}
	if len(ch.Id) > 0 && s.Generator != nil {
		id, er0 := s.Generator.Generate(ctx)
		if er0 == nil && len(id) > 0 {
			log[ch.Id] = id
		}
	}
	ext := BuildExt(ctx, ch.Ext)
	if len(ext) > 0 {
		for k, v := range ext {
			log[k] = v
		}
	}
	headers := BuildHeader(ctx, ch.Headers)
	msg, er1 := json.Marshal(log)
	if er1 != nil {
		return er1
	}
	if !s.Config.Goroutines {
		_, er3 := s.Producer.Produce(ctx, msg, headers)
		return er3
	} else {
		go s.Producer.Produce(ctx, msg, headers)
		return nil
	}
}
func BuildExt(ctx context.Context, keys *[]string) map[string]interface{} {
	headers := make(map[string]interface{})
	if keys != nil {
		hs := *keys
		for _, header := range hs {
			v := ctx.Value(header)
			if v != nil {
				headers[header] = v
			}
		}
	}
	return headers
}
func BuildHeader(ctx context.Context, keys *[]string) *map[string]string {
	if keys != nil {
		headers := make(map[string]string)
		hs := *keys
		for _, header := range hs {
			v := ctx.Value(header)
			if v != nil {
				s, ok := v.(string)
				if ok {
					headers[header] = s
				}
			}
		}
		if len(headers) > 0 {
			return &headers
		} else {
			return nil
		}
	}
	return nil
}
func GetString(ctx context.Context, key string) string {
	if len(key) > 0 {
		u := ctx.Value(key)
		if u != nil {
			s, ok := u.(string)
			if ok {
				return s
			} else {
				return ""
			}
		}
	}
	return ""
}
