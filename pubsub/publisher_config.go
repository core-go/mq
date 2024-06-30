package pubsub

type PublisherConfig struct {
	TopicId string       `yaml:"topic_id" mapstructure:"topic_id" json:"topicId,omitempty" gorm:"column:topicid" bson:"topicId,omitempty" dynamodbav:"topicId,omitempty" firestore:"topicId,omitempty"`
	Client  ClientConfig `yaml:"client" mapstructure:"client" json:"client,omitempty" gorm:"column:client" bson:"client,omitempty" dynamodbav:"client,omitempty" firestore:"client,omitempty"`
	Topic   *TopicConfig `yaml:"topic" mapstructure:"topic" json:"topic,omitempty" gorm:"column:topic" bson:"topic,omitempty" dynamodbav:"topic,omitempty" firestore:"topic,omitempty"`
	Retry   RetryConfig  `yaml:"retry" mapstructure:"retry" json:"retry,omitempty" gorm:"column:retry" bson:"retry,omitempty" dynamodbav:"retry,omitempty" firestore:"retry,omitempty"`
}

type TopicConfig struct {
	DelayThreshold int `yaml:"delay_threshold" mapstructure:"delay_threshold" json:"delayThreshold,omitempty" gorm:"column:delaythreshold" bson:"delayThreshold,omitempty" dynamodbav:"delayThreshold,omitempty" firestore:"delayThreshold,omitempty"` // MaxMessages
	CountThreshold int `yaml:"count_threshold" mapstructure:"" json:"countThreshold,omitempty" gorm:"column:countthreshold" bson:"countThreshold,omitempty" dynamodbav:"countThreshold,omitempty" firestore:"countThreshold,omitempty"`                // MaxMilliseconds
	ByteThreshold  int `yaml:"byte_threshold" mapstructure:"byte_threshold" json:"byteThreshold,omitempty" gorm:"column:bytethreshold" bson:"byteThreshold,omitempty" dynamodbav:"byteThreshold,omitempty" firestore:"byteThreshold,omitempty"`        // MaxBytes
	NumGoroutines  int `yaml:"num_goroutines" mapstructure:"num_goroutines" json:"numGoroutines,omitempty" gorm:"column:numgoroutines" bson:"numGoroutines,omitempty" dynamodbav:"numGoroutines,omitempty" firestore:"numGoroutines,omitempty"`
}
