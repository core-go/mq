package kafka

type ProducerConfig struct {
	Brokers         []string       `yaml:"brokers" mapstructure:"brokers" json:"brokers,omitempty" gorm:"column:brokers" bson:"brokers,omitempty" dynamodbav:"brokers,omitempty" firestore:"brokers,omitempty"`
	Topic           string         `yaml:"topic" mapstructure:"topic" json:"topic,omitempty" gorm:"column:topic" bson:"topic,omitempty" dynamodbav:"topic,omitempty" firestore:"topic,omitempty"`
	Timeout         int            `yaml:"timeout" mapstructure:"timeout" json:"timeout,omitempty" gorm:"column:timeout" bson:"timeout,omitempty" dynamodbav:"timeout,omitempty" firestore:"timeout,omitempty"`
	Client          ClientConfig   `yaml:"client" mapstructure:"client" json:"client,omitempty" gorm:"column:client" bson:"client,omitempty" dynamodbav:"client,omitempty" firestore:"client,omitempty"`
	MaxOpenRequests *int           `yaml:"max_open_requests" mapstructure:"max_open_requests" json:"maxOpenRequests,omitempty" gorm:"column:maxopenrequests" bson:"maxOpenRequests,omitempty" dynamodbav:"maxOpenRequests,omitempty" firestore:"maxOpenRequests,omitempty"`
	RequiredAcks    *int16         `yaml:"required_acks" mapstructure:"required_acks" json:"requiredAcks,omitempty" gorm:"column:requiredacks" bson:"requiredAcks,omitempty" dynamodbav:"requiredAcks,omitempty" firestore:"requiredAcks,omitempty"`
	Idempotent      *bool          `yaml:"idempotent" mapstructure:"idempotent" json:"idempotent,omitempty" gorm:"column:idempotent" bson:"idempotent,omitempty" dynamodbav:"idempotent,omitempty" firestore:"idempotent,omitempty"`
	ReturnSuccesses *bool          `yaml:"return_successes" mapstructure:"return_successes" json:"returnSuccesses,omitempty" gorm:"column:returnsuccesses" bson:"returnSuccesses,omitempty" dynamodbav:"returnSuccesses,omitempty" firestore:"returnSuccesses,omitempty"`
	Retry           *ProducerRetry `yaml:"retry" mapstructure:"retry" json:"retry,omitempty" gorm:"column:retry" bson:"retry,omitempty" dynamodbav:"retry,omitempty" firestore:"retry,omitempty"`
}

type ProducerRetry struct {
	Max     *int  `yaml:"max" mapstructure:"max" json:"max,omitempty" gorm:"column:max" bson:"max,omitempty" dynamodbav:"max,omitempty" firestore:"max,omitempty"`
	Backoff int64 `yaml:"backoff" mapstructure:"backoff" json:"backoff,omitempty" gorm:"column:backoff" bson:"backoff,omitempty" dynamodbav:"backoff,omitempty" firestore:"backoff,omitempty"`
}
