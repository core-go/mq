package kafka

type ProducerConfig struct {
	Brokers         []string       `mapstructure:"brokers" json:"brokers,omitempty" gorm:"column:brokers" bson:"brokers,omitempty" dynamodbav:"brokers,omitempty" firestore:"brokers,omitempty"`
	Topic           string         `mapstructure:"Topic" json:"Topic,omitempty" gorm:"column:Topic" bson:"Topic,omitempty" dynamodbav:"Topic,omitempty" firestore:"Topic,omitempty"`
	Client          ClientConfig   `mapstructure:"client" json:"client,omitempty" gorm:"column:client" bson:"client,omitempty" dynamodbav:"client,omitempty" firestore:"client,omitempty"`
	MaxOpenRequests *int           `mapstructure:"max_open_requests" json:"maxOpenRequests,omitempty" gorm:"column:maxopenrequests" bson:"maxOpenRequests,omitempty" dynamodbav:"maxOpenRequests,omitempty" firestore:"maxOpenRequests,omitempty"`
	RequiredAcks    *int16         `mapstructure:"required_acks" json:"requiredAcks,omitempty" gorm:"column:requiredacks" bson:"requiredAcks,omitempty" dynamodbav:"requiredAcks,omitempty" firestore:"requiredAcks,omitempty"`
	Idempotent      *bool          `mapstructure:"idempotent" json:"idempotent,omitempty" gorm:"column:idempotent" bson:"idempotent,omitempty" dynamodbav:"idempotent,omitempty" firestore:"idempotent,omitempty"`
	ReturnSuccesses *bool          `mapstructure:"return_successes" json:"returnSuccesses,omitempty" gorm:"column:returnsuccesses" bson:"returnSuccesses,omitempty" dynamodbav:"returnSuccesses,omitempty" firestore:"returnSuccesses,omitempty"`
	Retry           *ProducerRetry `mapstructure:"retry" json:"retry,omitempty" gorm:"column:retry" bson:"retry,omitempty" dynamodbav:"retry,omitempty" firestore:"retry,omitempty"`
}

type ProducerRetry struct {
	Max     *int  `mapstructure:"max" json:"max,omitempty" gorm:"column:max" bson:"max,omitempty" dynamodbav:"max,omitempty" firestore:"max,omitempty"`
	Backoff int64 `mapstructure:"backoff" json:"backoff,omitempty" gorm:"column:backoff" bson:"backoff,omitempty" dynamodbav:"backoff,omitempty" firestore:"backoff,omitempty"`
}
