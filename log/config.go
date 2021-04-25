package log

import "github.com/sirupsen/logrus"

type Config struct {
	Level           string           `mapstructure:"level" json:"level,omitempty" gorm:"column:level" bson:"level,omitempty" dynamodbav:"level,omitempty" firestore:"level,omitempty"`
	Duration        string           `mapstructure:"duration" json:"duration,omitempty" gorm:"column:duration" bson:"duration,omitempty" dynamodbav:"duration,omitempty" firestore:"duration,omitempty"`
	Fields          string           `mapstructure:"fields" json:"fields,omitempty" gorm:"column:fields" bson:"fields,omitempty" dynamodbav:"fields,omitempty" firestore:"fields,omitempty"`
	FieldMap        string           `mapstructure:"field_map" json:"fieldMap,omitempty" gorm:"column:fieldmap" bson:"fieldMap,omitempty" dynamodbav:"fieldMap,omitempty" firestore:"fieldMap,omitempty"`
	Map             *logrus.FieldMap `mapstructure:"map" json:"map,omitempty" gorm:"column:map" bson:"map,omitempty" dynamodbav:"map,omitempty" firestore:"map,omitempty"`
	TimestampFormat string           `mapstructure:"timestamp_format" json:"timestampFormat,omitempty" gorm:"column:timestampformat" bson:"timestampFormat,omitempty" dynamodbav:"timestampFormat,omitempty" firestore:"timestampFormat,omitempty"`
}

type FieldConfig struct {
	FieldMap string    `mapstructure:"field_map" json:"fieldMap,omitempty" gorm:"column:fieldmap" bson:"fieldMap,omitempty" dynamodbav:"fieldMap,omitempty" firestore:"fieldMap,omitempty"`
	Duration string    `mapstructure:"duration" json:"duration,omitempty" gorm:"column:duration" bson:"duration,omitempty" dynamodbav:"duration,omitempty" firestore:"duration,omitempty"`
	Fields   *[]string `mapstructure:"fields" json:"fields,omitempty" gorm:"column:fields" bson:"fields,omitempty" dynamodbav:"fields,omitempty" firestore:"fields,omitempty"`
}
