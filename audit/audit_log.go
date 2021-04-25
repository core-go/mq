package audit

import (
	"context"
	"encoding/json"
	"time"
)

func NewAuditLogSender(send func(context.Context, []byte, map[string]string) (string, error), config AuditLogConfig, schema AuditLogSchema, options ...func(context.Context) (string, error)) *AuditLogSender {
	var generate func(context.Context) (string, error)
	if len(options) >= 1 {
		generate = options[0]
	}
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
	sender := AuditLogSender{send: send, Config: config, Schema: schema, Generate: generate}
	return &sender
}

type AuditLogConfig struct {
	User       string `mapstructure:"user" json:"user,omitempty" gorm:"column:user" bson:"user,omitempty" dynamodbav:"user,omitempty" firestore:"user,omitempty"`
	Ip         string `mapstructure:"ip" json:"ip,omitempty" gorm:"column:ip" bson:"ip,omitempty" dynamodbav:"ip,omitempty" firestore:"ip,omitempty"`
	True       string `mapstructure:"true" json:"true,omitempty" gorm:"column:true" bson:"true,omitempty" dynamodbav:"true,omitempty" firestore:"true,omitempty"`
	False      string `mapstructure:"false" json:"false,omitempty" gorm:"column:false" bson:"false,omitempty" dynamodbav:"false,omitempty" firestore:"false,omitempty"`
	Goroutines bool   `mapstructure:"goroutines" json:"goroutines,omitempty" gorm:"column:goroutines" bson:"goroutines,omitempty" dynamodbav:"goroutines,omitempty" firestore:"goroutines,omitempty"`
}
type AuditLogSchema struct {
	Id        string   `mapstructure:"id" json:"id,omitempty" gorm:"column:id" bson:"_id,omitempty" dynamodbav:"id,omitempty" firestore:"id,omitempty"`
	User      string   `mapstructure:"user" json:"user,omitempty" gorm:"column:user" bson:"user,omitempty" dynamodbav:"user,omitempty" firestore:"user,omitempty"`
	Ip        string   `mapstructure:"ip" json:"ip,omitempty" gorm:"column:ip" bson:"ip,omitempty" dynamodbav:"ip,omitempty" firestore:"ip,omitempty"`
	Resource  string   `mapstructure:"resource" json:"resource,omitempty" gorm:"column:resource" bson:"resource,omitempty" dynamodbav:"resource,omitempty" firestore:"resource,omitempty"`
	Action    string   `mapstructure:"action" json:"action,omitempty" gorm:"column:action" bson:"action,omitempty" dynamodbav:"action,omitempty" firestore:"action,omitempty"`
	Timestamp string   `mapstructure:"timestamp" json:"timestamp,omitempty" gorm:"column:timestamp" bson:"timestamp,omitempty" dynamodbav:"timestamp,omitempty" firestore:"timestamp,omitempty"`
	Status    string   `mapstructure:"status" json:"status,omitempty" gorm:"column:status" bson:"status,omitempty" dynamodbav:"status,omitempty" firestore:"status,omitempty"`
	Desc      string   `mapstructure:"desc" json:"desc,omitempty" gorm:"column:desc" bson:"desc,omitempty" dynamodbav:"desc,omitempty" firestore:"desc,omitempty"`
	Ext       []string `mapstructure:"ext" json:"ext,omitempty" gorm:"column:ext" bson:"ext,omitempty" dynamodbav:"ext,omitempty" firestore:"ext,omitempty"`
	Headers   []string `mapstructure:"headers" json:"headers,omitempty" gorm:"column:headers" bson:"headers,omitempty" dynamodbav:"headers,omitempty" firestore:"headers,omitempty"`
}
type AuditLogSender struct {
	send     func(ctx context.Context, data []byte, attributes map[string]string) (string, error)
	Config   AuditLogConfig
	Schema   AuditLogSchema
	Generate func(ctx context.Context) (string, error)
}

func (s *AuditLogSender) Write(ctx context.Context, resource string, action string, success bool, desc string) error {
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
	log[ch.User] = getString(ctx, s.Config.User)
	if len(ch.Ip) > 0 {
		log[ch.Ip] = getString(ctx, s.Config.Ip)
	}
	if len(ch.Id) > 0 && s.Generate != nil {
		id, er0 := s.Generate(ctx)
		if er0 == nil && len(id) > 0 {
			log[ch.Id] = id
		}
	}
	ext := buildExt(ctx, ch.Ext)
	if len(ext) > 0 {
		for k, v := range ext {
			log[k] = v
		}
	}
	headers := buildHeader(ctx, ch.Headers)
	msg, er1 := json.Marshal(log)
	if er1 != nil {
		return er1
	}
	if !s.Config.Goroutines {
		_, er3 := s.send(ctx, msg, headers)
		return er3
	} else {
		go s.send(ctx, msg, headers)
		return nil
	}
}
func buildExt(ctx context.Context, keys []string) map[string]interface{} {
	headers := make(map[string]interface{})
	if keys != nil {
		for _, header := range keys {
			v := ctx.Value(header)
			if v != nil {
				headers[header] = v
			}
		}
	}
	return headers
}
func buildHeader(ctx context.Context, keys []string) map[string]string {
	if keys != nil {
		headers := make(map[string]string)
		for _, header := range keys {
			v := ctx.Value(header)
			if v != nil {
				s, ok := v.(string)
				if ok {
					headers[header] = s
				}
			}
		}
		if len(headers) > 0 {
			return headers
		} else {
			return nil
		}
	}
	return nil
}
func getString(ctx context.Context, key string) string {
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
