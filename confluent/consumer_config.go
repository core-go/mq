package kafka

type ConsumerConfig struct {
	Brokers        []string     `yaml:"brokers" mapstructure:"brokers" json:"brokers,omitempty" gorm:"column:brokers" bson:"brokers,omitempty" dynamodbav:"brokers,omitempty" firestore:"brokers,omitempty"`
	GroupID        string       `yaml:"group_id" mapstructure:"group_id" json:"groupID,omitempty" gorm:"column:groupid" bson:"groupID,omitempty" dynamodbav:"groupID,omitempty" firestore:"groupID,omitempty"`
	Topic          string       `yaml:"topic" mapstructure:"topic" json:"topic,omitempty" gorm:"column:topic" bson:"topic,omitempty" dynamodbav:"topic,omitempty" firestore:"topic,omitempty"`
	Client         ClientConfig `yaml:"client" mapstructure:"client" json:"client,omitempty" gorm:"column:client" bson:"client,omitempty" dynamodbav:"client,omitempty" firestore:"client,omitempty"`
	InitialOffsets *int64       `yaml:"initial_offsets" mapstructure:"initial_offsets" json:"initialOffsets,omitempty" gorm:"column:initialoffsets" bson:"initialOffsets,omitempty" dynamodbav:"initialOffsets,omitempty" firestore:"initialOffsets,omitempty"`
	AckOnConsume   bool         `yaml:"ack" mapstructure:"ack" json:"ack,omitempty" gorm:"column:ack" bson:"ack,omitempty" dynamodbav:"ack,omitempty" firestore:"ack,omitempty"`
}
