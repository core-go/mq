package kafka

type ConsumerConfig struct {
	Brokers        []string     `mapstructure:"brokers" json:"brokers,omitempty" gorm:"column:brokers" bson:"brokers,omitempty" dynamodbav:"brokers,omitempty" firestore:"brokers,omitempty"`
	GroupID        string       `mapstructure:"group_id" json:"groupID,omitempty" gorm:"column:groupid" bson:"groupID,omitempty" dynamodbav:"groupID,omitempty" firestore:"groupID,omitempty"`
	Topic          string       `mapstructure:"Topic" json:"Topic,omitempty" gorm:"column:Topic" bson:"Topic,omitempty" dynamodbav:"Topic,omitempty" firestore:"Topic,omitempty"`
	Client         ClientConfig `mapstructure:"client" json:"client,omitempty" gorm:"column:client" bson:"client,omitempty" dynamodbav:"client,omitempty" firestore:"client,omitempty"`
	InitialOffsets *int64       `mapstructure:"initial_offsets" json:"initialOffsets,omitempty" gorm:"column:initialoffsets" bson:"initialOffsets,omitempty" dynamodbav:"initialOffsets,omitempty" firestore:"initialOffsets,omitempty"`
	AckOnConsume   bool         `mapstructure:"ack" json:"ack,omitempty" gorm:"column:ack" bson:"ack,omitempty" dynamodbav:"ack,omitempty" firestore:"ack,omitempty"`
}
