package mq

type BatchWorkerConfig struct {
	BatchSize  int   `mapstructure:"batch_size" json:"batchSize,omitempty" gorm:"column:batchsize" bson:"batchSize,omitempty" dynamodbav:"batchSize,omitempty" firestore:"batchSize,omitempty"`
	Timeout    int64 `mapstructure:"timeout" json:"timeout,omitempty" gorm:"column:timeout" bson:"timeout,omitempty" dynamodbav:"timeout,omitempty" firestore:"timeout,omitempty"`
	LimitRetry int   `mapstructure:"limit_retry" json:"limitRetry,omitempty" gorm:"column:limitretry" bson:"limitRetry,omitempty" dynamodbav:"limitRetry,omitempty" firestore:"limitRetry,omitempty"`
	Goroutines bool  `mapstructure:"goroutines" json:"goroutines,omitempty" gorm:"column:goroutines" bson:"goroutines,omitempty" dynamodbav:"goroutines,omitempty" firestore:"goroutines,omitempty"`
}
