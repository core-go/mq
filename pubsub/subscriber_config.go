package pubsub

type SubscriberConfig struct {
	SubscriptionId     string             `yaml:"subscription_id" mapstructure:"subscription_id" json:"subscriptionId,omitempty" gorm:"column:subscriptionid" bson:"subscriptionId,omitempty" dynamodbav:"subscriptionId,omitempty" firestore:"subscriptionId,omitempty"`
	Client             ClientConfig       `yaml:"client" mapstructure:"client" json:"client,omitempty" gorm:"column:client" bson:"client,omitempty" dynamodbav:"client,omitempty" firestore:"client,omitempty"`
	SubscriptionConfig SubscriptionConfig `yaml:"subscription" mapstructure:"subscription" json:"subscription,omitempty" gorm:"column:subscription" bson:"subscription,omitempty" dynamodbav:"subscription,omitempty" firestore:"subscription,omitempty"`
	Retry              RetryConfig        `yaml:"retry" mapstructure:"retry" json:"retry,omitempty" gorm:"column:retry" bson:"retry,omitempty" dynamodbav:"retry,omitempty" firestore:"retry,omitempty"`
}

type SubscriptionConfig struct {
	MaxOutstandingMessages int `yaml:"max_outstanding_messages" mapstructure:"max_outstanding_messages" json:"maxOutstandingMessages,omitempty" gorm:"column:maxoutstandingmessages" bson:"maxOutstandingMessages,omitempty" dynamodbav:"maxOutstandingMessages,omitempty" firestore:"maxOutstandingMessages,omitempty"`
	NumGoroutines          int `yaml:"num_goroutines" mapstructure:"num_goroutines" json:"numGoroutines,omitempty" gorm:"column:numgoroutines" bson:"numGoroutines,omitempty" dynamodbav:"numGoroutines,omitempty" firestore:"numGoroutines,omitempty"`
}
