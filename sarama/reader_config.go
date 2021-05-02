package kafka

type ReaderConfig struct {
	Brokers        []string     `mapstructure:"brokers" json:"brokers,omitempty" gorm:"column:brokers" bson:"brokers,omitempty" dynamodbav:"brokers,omitempty" firestore:"brokers,omitempty"`
	GroupID        string       `mapstructure:"group_id" json:"groupID,omitempty" gorm:"column:groupid" bson:"groupID,omitempty" dynamodbav:"groupID,omitempty" firestore:"groupID,omitempty"`
	Topic          string       `mapstructure:"topic" json:"topic,omitempty" gorm:"column:topic" bson:"topic,omitempty" dynamodbav:"topic,omitempty" firestore:"topic,omitempty"`
	Client         ClientConfig `mapstructure:"client" json:"client,omitempty" gorm:"column:client" bson:"client,omitempty" dynamodbav:"client,omitempty" firestore:"client,omitempty"`
	InitialOffsets *int64       `mapstructure:"initial_offsets" json:"initialOffsets,omitempty" gorm:"column:initialoffsets" bson:"initialOffsets,omitempty" dynamodbav:"initialOffsets,omitempty" firestore:"initialOffsets,omitempty"`
}
