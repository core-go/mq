package log

type Config struct {
	Level    string    `mapstructure:"level" json:"level,omitempty" gorm:"column:level" bson:"level,omitempty" dynamodbav:"level,omitempty" firestore:"level,omitempty"`
	Duration string    `mapstructure:"duration" json:"duration,omitempty" gorm:"column:duration" bson:"duration,omitempty" dynamodbav:"duration,omitempty" firestore:"duration,omitempty"`
	Fields   string    `mapstructure:"fields" json:"fields,omitempty" gorm:"column:fields" bson:"fields,omitempty" dynamodbav:"fields,omitempty" firestore:"fields,omitempty"`
	FieldMap string    `mapstructure:"field_map" json:"fieldMap,omitempty" gorm:"column:fieldmap" bson:"fieldMap,omitempty" dynamodbav:"fieldMap,omitempty" firestore:"fieldMap,omitempty"`
	Map      *FieldMap `mapstructure:"map" json:"map,omitempty" gorm:"column:map" bson:"map,omitempty" dynamodbav:"map,omitempty" firestore:"map,omitempty"`
}

type FieldMap struct {
	Time       string `mapstructure:"time" json:"time,omitempty" gorm:"column:time" bson:"time,omitempty" dynamodbav:"time,omitempty" firestore:"time,omitempty"`
	Level      string `mapstructure:"level" json:"level,omitempty" gorm:"column:level" bson:"level,omitempty" dynamodbav:"level,omitempty" firestore:"level,omitempty"`
	Name       string `mapstructure:"name" json:"name,omitempty" gorm:"column:name" bson:"name,omitempty" dynamodbav:"name,omitempty" firestore:"name,omitempty"`
	Caller     string `mapstructure:"caller" json:"caller,omitempty" gorm:"column:caller" bson:"caller,omitempty" dynamodbav:"caller,omitempty" firestore:"caller,omitempty"`
	Msg        string `mapstructure:"msg" json:"msg,omitempty" gorm:"column:msg" bson:"msg,omitempty" dynamodbav:"msg,omitempty" firestore:"msg,omitempty"`
	Stacktrace string `mapstructure:"stacktrace" json:"stacktrace,omitempty" gorm:"column:stacktrace" bson:"stacktrace,omitempty" dynamodbav:"stacktrace,omitempty" firestore:"stacktrace,omitempty"`
}

type FieldConfig struct {
	FieldMap string    `mapstructure:"field_map" json:"fieldMap,omitempty" gorm:"column:fieldmap" bson:"fieldMap,omitempty" dynamodbav:"fieldMap,omitempty" firestore:"fieldMap,omitempty"`
	Duration string    `mapstructure:"duration" json:"duration,omitempty" gorm:"column:duration" bson:"duration,omitempty" dynamodbav:"duration,omitempty" firestore:"duration,omitempty"`
	Fields   *[]string `mapstructure:"fields" json:"fields,omitempty" gorm:"column:fields" bson:"fields,omitempty" dynamodbav:"fields,omitempty" firestore:"fields,omitempty"`
}
