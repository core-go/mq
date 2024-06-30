package rabbitmq

type ConsumerConfig struct {
	Url          string `yaml:"url" mapstructure:"url" json:"url,omitempty" gorm:"column:url" bson:"url,omitempty" dynamodbav:"url,omitempty" firestore:"url,omitempty"`
	ExchangeName string `yaml:"exchange_name" mapstructure:"exchange_name" json:"exchangeName,omitempty" gorm:"column:exchangename" bson:"exchangeName,omitempty" dynamodbav:"exchangeName,omitempty" firestore:"exchangeName,omitempty"`
	ExchangeKind string `yaml:"exchange_kind" mapstructure:"exchange_kind" json:"exchangeKind,omitempty" gorm:"column:exchangekind" bson:"exchangeKind,omitempty" dynamodbav:"exchangeKind,omitempty" firestore:"exchangeKind,omitempty"`
	QueueName    string `yaml:"queue_name" mapstructure:"queue_name" json:"queueName,omitempty" gorm:"column:queuename" bson:"queueName,omitempty" dynamodbav:"queueName,omitempty" firestore:"queueName,omitempty"`
	AutoDelete   bool   `yaml:"auto_delete" mapstructure:"auto_delete" json:"autoDelete,omitempty" gorm:"column:autodelete" bson:"autoDelete,omitempty" dynamodbav:"autoDelete,omitempty" firestore:"autoDelete,omitempty"`
}
