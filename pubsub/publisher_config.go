package pubsub

type PublisherConfig struct {
	TopicId string       `mapstructure:"topic_id" json:"topicId,omitempty" gorm:"column:topicid" bson:"topicId,omitempty" dynamodbav:"topicId,omitempty" firestore:"topicId,omitempty"`
	Client  ClientConfig `mapstructure:"client" json:"client,omitempty" gorm:"column:client" bson:"client,omitempty" dynamodbav:"client,omitempty" firestore:"client,omitempty"`
	Topic   TopicConfig  `mapstructure:"topic" json:"topic,omitempty" gorm:"column:topic" bson:"topic,omitempty" dynamodbav:"topic,omitempty" firestore:"topic,omitempty"`
	Retry   RetryConfig  `mapstructure:"retry" json:"retry,omitempty" gorm:"column:retry" bson:"retry,omitempty" dynamodbav:"retry,omitempty" firestore:"retry,omitempty"`
}

type TopicConfig struct {
	DelayThreshold int `mapstructure:"delay_threshold" json:"delayThreshold,omitempty" gorm:"column:delaythreshold" bson:"delayThreshold,omitempty" dynamodbav:"delayThreshold,omitempty" firestore:"delayThreshold,omitempty"` // MaxMessages
	CountThreshold int `mapstructure:"count_threshold" json:"countThreshold,omitempty" gorm:"column:countthreshold" bson:"countThreshold,omitempty" dynamodbav:"countThreshold,omitempty" firestore:"countThreshold,omitempty"` // MaxMilliseconds
	ByteThreshold  int `mapstructure:"byte_threshold" json:"byteThreshold,omitempty" gorm:"column:bytethreshold" bson:"byteThreshold,omitempty" dynamodbav:"byteThreshold,omitempty" firestore:"byteThreshold,omitempty"`  // MaxBytes
	NumGoroutines  int `mapstructure:"num_goroutines" json:"numGoroutines,omitempty" gorm:"column:numgoroutines" bson:"numGoroutines,omitempty" dynamodbav:"numGoroutines,omitempty" firestore:"numGoroutines,omitempty"`
}
