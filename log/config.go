package log

import "github.com/sirupsen/logrus"

type Config struct {
	Level           string           `yaml:"level" mapstructure:"level" json:"level,omitempty" gorm:"column:level" bson:"level,omitempty" dynamodbav:"level,omitempty" firestore:"level,omitempty"`
	Output          string           `yaml:"output" mapstructure:"output" json:"output,omitempty" gorm:"column:output" bson:"output,omitempty" dynamodbav:"output,omitempty" firestore:"output,omitempty"`
	Duration        string           `yaml:"duration" mapstructure:"duration" json:"duration,omitempty" gorm:"column:duration" bson:"duration,omitempty" dynamodbav:"duration,omitempty" firestore:"duration,omitempty"`
	Fields          string           `yaml:"fields" mapstructure:"fields" json:"fields,omitempty" gorm:"column:fields" bson:"fields,omitempty" dynamodbav:"fields,omitempty" firestore:"fields,omitempty"`
	FieldMap        string           `yaml:"field_map" mapstructure:"field_map" json:"fieldMap,omitempty" gorm:"column:fieldmap" bson:"fieldMap,omitempty" dynamodbav:"fieldMap,omitempty" firestore:"fieldMap,omitempty"`
	Map             *logrus.FieldMap `yaml:"map" mapstructure:"map" json:"map,omitempty" gorm:"column:map" bson:"map,omitempty" dynamodbav:"map,omitempty" firestore:"map,omitempty"`
	TimestampFormat string           `yaml:"timestamp_format" mapstructure:"timestamp_format" json:"timestampFormat,omitempty" gorm:"column:timestampformat" bson:"timestampFormat,omitempty" dynamodbav:"timestampFormat,omitempty" firestore:"timestampFormat,omitempty"`
}

type FieldConfig struct {
	FieldMap string    `yaml:"field_map" mapstructure:"field_map" json:"fieldMap,omitempty" gorm:"column:fieldmap" bson:"fieldMap,omitempty" dynamodbav:"fieldMap,omitempty" firestore:"fieldMap,omitempty"`
	Duration string    `yaml:"duration" mapstructure:"duration" json:"duration,omitempty" gorm:"column:duration" bson:"duration,omitempty" dynamodbav:"duration,omitempty" firestore:"duration,omitempty"`
	Fields   *[]string `yaml:"fields" mapstructure:"fields" json:"fields,omitempty" gorm:"column:fields" bson:"fields,omitempty" dynamodbav:"fields,omitempty" firestore:"fields,omitempty"`
}
