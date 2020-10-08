package mq

import (
	"context"
	"encoding/json"
	"time"
)

type ActivityLog struct {
	Id        string     `mapstructure:"id" json:"id,omitempty" gorm:"column:id" bson:"_id,omitempty" dynamodbav:"id,omitempty" firestore:"id,omitempty"`
	User      string     `mapstructure:"user" json:"user,omitempty" gorm:"column:user" bson:"user,omitempty" dynamodbav:"user,omitempty" firestore:"user,omitempty"`
	Ip        string     `mapstructure:"ip" json:"ip,omitempty" gorm:"column:ip" bson:"ip,omitempty" dynamodbav:"ip,omitempty" firestore:"ip,omitempty"`
	Resource  string     `mapstructure:"resource" json:"resource,omitempty" gorm:"column:resource" bson:"resource,omitempty" dynamodbav:"resource,omitempty" firestore:"resource,omitempty"`
	Action    string     `mapstructure:"action" json:"action,omitempty" gorm:"column:action" bson:"action,omitempty" dynamodbav:"action,omitempty" firestore:"action,omitempty"`
	Timestamp *time.Time `mapstructure:"timestamp" json:"timestamp,omitempty" gorm:"column:timestamp" bson:"timestamp,omitempty" dynamodbav:"timestamp,omitempty" firestore:"timestamp,omitempty"`
	Status    string     `mapstructure:"status" json:"status,omitempty" gorm:"column:status" bson:"status,omitempty" dynamodbav:"status,omitempty" firestore:"status,omitempty"`
	Desc      string     `mapstructure:"desc" json:"desc,omitempty" gorm:"column:desc" bson:"desc,omitempty" dynamodbav:"desc,omitempty" firestore:"desc,omitempty"`
}

type IdGenerator interface {
	Generate(ctx context.Context) (string, error)
}

func NewActivityLogSender(producer Producer, config ActivityLogConfig, generator IdGenerator, header *[]string) *ActivityLogSender {
	sender := ActivityLogSender{producer, config, generator, header}
	return &sender
}

type ActivityLogSender struct {
	Producer  Producer
	Config    ActivityLogConfig
	Generator IdGenerator
	Headers   *[]string
}

type ActivityLogConfig struct {
	User       string `mapstructure:"user" json:"user,omitempty" gorm:"column:user" bson:"user,omitempty" dynamodbav:"user,omitempty" firestore:"user,omitempty"`
	Ip         string `mapstructure:"ip" json:"ip,omitempty" gorm:"column:ip" bson:"ip,omitempty" dynamodbav:"ip,omitempty" firestore:"ip,omitempty"`
	True       string `mapstructure:"true" json:"true,omitempty" gorm:"column:true" bson:"true,omitempty" dynamodbav:"true,omitempty" firestore:"true,omitempty"`
	False      string `mapstructure:"false" json:"false,omitempty" gorm:"column:false" bson:"false,omitempty" dynamodbav:"false,omitempty" firestore:"false,omitempty"`
	Goroutines bool   `mapstructure:"goroutines" json:"goroutines,omitempty" gorm:"column:goroutines" bson:"goroutines,omitempty" dynamodbav:"goroutines,omitempty" firestore:"goroutines,omitempty"`
}

func (s *ActivityLogSender) SaveLog(ctx context.Context, resource string, action string, success bool, desc string) error {
	log := ActivityLog{}
	now := time.Now()
	log.Timestamp = &now
	log.Resource = resource
	log.Action = action
	log.Desc = desc
	if success {
		log.Status = s.Config.True
	} else {
		log.Status = s.Config.False
	}
	log.User = GetString(ctx, s.Config.User)
	log.Ip = GetString(ctx, s.Config.Ip)
	if s.Generator != nil {
		id, er0 := s.Generator.Generate(ctx)
		if er0 == nil {
			log.Id = id
		}
	}
	headers := BuildHeader(ctx, s.Headers)
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

func BuildHeader(ctx context.Context, keys *[]string) *map[string]string {
	headers := make(map[string]string)
	if keys != nil {
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
	}
	if len(headers) > 0 {
		return &headers
	} else {
		return nil
	}
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
