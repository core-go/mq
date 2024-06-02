package kafka

type ReaderConfig struct {
	Brokers        []string     `yaml:"brokers" mapstructure:"brokers" json:"brokers,omitempty" gorm:"column:brokers" bson:"brokers,omitempty" dynamodbav:"brokers,omitempty" firestore:"brokers,omitempty"`
	GroupID        string       `yaml:"group_id" mapstructure:"group_id" json:"groupID,omitempty" gorm:"column:groupid" bson:"groupID,omitempty" dynamodbav:"groupID,omitempty" firestore:"groupID,omitempty"`
	Topic          string       `yaml:"topic" mapstructure:"topic" json:"topic,omitempty" gorm:"column:topic" bson:"topic,omitempty" dynamodbav:"topic,omitempty" firestore:"topic,omitempty"`
	Client         ClientConfig `yaml:"client" mapstructure:"client" json:"client,omitempty" gorm:"column:client" bson:"client,omitempty" dynamodbav:"client,omitempty" firestore:"client,omitempty"`
	MinBytes       *int         `yaml:"min_bytes" mapstructure:"min_bytes" json:"minBytes,omitempty" gorm:"column:minbytes" bson:"minBytes,omitempty" dynamodbav:"minBytes,omitempty" firestore:"minBytes,omitempty"`
	MaxBytes       int          `yaml:"max_bytes" mapstructure:"max_bytes" json:"maxBytes,omitempty" gorm:"column:maxbytes" bson:"maxBytes,omitempty" dynamodbav:"maxBytes,omitempty" firestore:"maxBytes,omitempty"`
	CommitInterval *int64       `yaml:"commit_interval" mapstructure:"commit_interval" json:"commitInterval,omitempty" gorm:"column:commitinterval" bson:"commitInterval,omitempty" dynamodbav:"commitInterval,omitempty" firestore:"commitInterval,omitempty"`
	Key            string       `yaml:"key" mapstructure:"key" json:"key,omitempty" gorm:"column:key" bson:"key,omitempty" dynamodbav:"key,omitempty" firestore:"key,omitempty"`
}
