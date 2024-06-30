package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type (
	Config struct {
		Region          string `yaml:"region" mapstructure:"region" json:"region,omitempty" gorm:"column:region" bson:"region,omitempty" dynamodbav:"region,omitempty" firestore:"region,omitempty"`
		AccessKeyID     string `yaml:"access_key_id" mapstructure:"access_key_id" json:"accessKeyID,omitempty" gorm:"column:accessKeyID" bson:"accessKeyID,omitempty" dynamodbav:"accessKeyID,omitempty" firestore:"accessKeyID,omitempty"`
		SecretAccessKey string `yaml:"secret_access_key" mapstructure:"secret_access_key" json:"secretAccessKey,omitempty" gorm:"column:secretaccesskey" bson:"secretAccessKey,omitempty" dynamodbav:"secretAccessKey,omitempty" firestore:"secretAccessKey,omitempty"`
		QueueName       string `yaml:"a" mapstructure:"queue_name" json:"queueName,omitempty" gorm:"column:token" bson:"queueName,omitempty" dynamodbav:"queueName,omitempty" firestore:"queueName,omitempty"`
	}
)

func NewSession(config Config) (*session.Session, error) {
	c := &aws.Config{
		Region:      aws.String(config.Region),
		Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.SecretAccessKey, ""),
	}
	return session.NewSession(c)
}

func Connect(config Config) (*sqs.SQS, error) {
	sess, err := NewSession(config)
	if err != nil {
		return nil, err
	}
	mySQS := sqs.New(sess)
	return mySQS, nil
}

func ConnectWithSession(session *session.Session) *sqs.SQS {
	return sqs.New(session)
}
