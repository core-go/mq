package kafka

type WriterConfig struct {
	Brokers []string     `yaml:"brokers" mapstructure:"brokers" json:"brokers,omitempty" gorm:"column:brokers" bson:"brokers,omitempty" dynamodbav:"brokers,omitempty" firestore:"brokers,omitempty"`
	Topic   string       `yaml:"topic" mapstructure:"topic" json:"topic,omitempty" gorm:"column:topic" bson:"topic,omitempty" dynamodbav:"topic,omitempty" firestore:"topic,omitempty"`
	Client  ClientConfig `yaml:"client" mapstructure:"client" json:"client,omitempty" gorm:"column:client" bson:"client,omitempty" dynamodbav:"client,omitempty" firestore:"client,omitempty"`
}
