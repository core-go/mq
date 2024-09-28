package log

type Config struct {
	Level       string     `yaml:"level" mapstructure:"level" json:"level,omitempty" gorm:"column:level" bson:"level,omitempty" dynamodbav:"level,omitempty" firestore:"level,omitempty"`
	Duration    string     `yaml:"duration" mapstructure:"duration" json:"duration,omitempty" gorm:"column:duration" bson:"duration,omitempty" dynamodbav:"duration,omitempty" firestore:"duration,omitempty"`
	Fields      string     `yaml:"fields" mapstructure:"fields" json:"fields,omitempty" gorm:"column:fields" bson:"fields,omitempty" dynamodbav:"fields,omitempty" firestore:"fields,omitempty"`
	FieldMap    string     `yaml:"field_map" mapstructure:"field_map" json:"fieldMap,omitempty" gorm:"column:fieldmap" bson:"fieldMap,omitempty" dynamodbav:"fieldMap,omitempty" firestore:"fieldMap,omitempty"`
	Map         *FieldMap  `yaml:"map" mapstructure:"map" json:"map,omitempty" gorm:"column:map" bson:"map,omitempty" dynamodbav:"map,omitempty" firestore:"map,omitempty"`
	CallerLevel string     `yaml:"caller_level" mapstructure:"caller_level" json:"callerLevel,omitempty" gorm:"column:callerlevel" bson:"callerLevel,omitempty" dynamodbav:"callerLevel,omitempty" firestore:"callerLevel,omitempty"`
	CallerSkip  int        `yaml:"caller_skip" mapstructure:"caller_skip" json:"callerSkip,omitempty" gorm:"column:callerskip" bson:"callerSkip,omitempty" dynamodbav:"callerSkip,omitempty" firestore:"callerSkip,omitempty"`
	Output      string     `yaml:"output" mapstructure:"output" json:"output,omitempty" gorm:"column:output" bson:"output,omitempty" dynamodbav:"output,omitempty" firestore:"output,omitempty"`
	MaxSize     SizeOfFile `yaml:"max_file_size" mapstructure:"max_file_size" json:"max_file_size,omitempty" gorm:"column:max_file_size" bson:"max_file_size,omitempty" dynamodbav:"max_file_size,omitempty" firestore:"max_file_size,omitempty"`
}

type FieldMap struct {
	Time       string `yaml:"time" mapstructure:"time" json:"time,omitempty" gorm:"column:time" bson:"time,omitempty" dynamodbav:"time,omitempty" firestore:"time,omitempty"`
	Level      string `yaml:"level" mapstructure:"level" json:"level,omitempty" gorm:"column:level" bson:"level,omitempty" dynamodbav:"level,omitempty" firestore:"level,omitempty"`
	Name       string `yaml:"name" mapstructure:"name" json:"name,omitempty" gorm:"column:name" bson:"name,omitempty" dynamodbav:"name,omitempty" firestore:"name,omitempty"`
	Caller     string `yaml:"caller" mapstructure:"caller" json:"caller,omitempty" gorm:"column:caller" bson:"caller,omitempty" dynamodbav:"caller,omitempty" firestore:"caller,omitempty"`
	Function   string `yaml:"function" mapstructure:"function" json:"function,omitempty" gorm:"column:function" bson:"function,omitempty" dynamodbav:"function,omitempty" firestore:"function,omitempty"`
	Msg        string `yaml:"msg" mapstructure:"msg" json:"msg,omitempty" gorm:"column:msg" bson:"msg,omitempty" dynamodbav:"msg,omitempty" firestore:"msg,omitempty"`
	Stacktrace string `yaml:"stacktrace" mapstructure:"stacktrace" json:"stacktrace,omitempty" gorm:"column:stacktrace" bson:"stacktrace,omitempty" dynamodbav:"stacktrace,omitempty" firestore:"stacktrace,omitempty"`
}

type FieldConfig struct {
	FieldMap string    `yaml:"field_map" mapstructure:"field_map" json:"fieldMap,omitempty" gorm:"column:fieldmap" bson:"fieldMap,omitempty" dynamodbav:"fieldMap,omitempty" firestore:"fieldMap,omitempty"`
	Duration string    `yaml:"duration" mapstructure:"duration" json:"duration,omitempty" gorm:"column:duration" bson:"duration,omitempty" dynamodbav:"duration,omitempty" firestore:"duration,omitempty"`
	Fields   *[]string `yaml:"fields" mapstructure:"fields" json:"fields,omitempty" gorm:"column:fields" bson:"fields,omitempty" dynamodbav:"fields,omitempty" firestore:"fields,omitempty"`
}
