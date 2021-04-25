package rabbitmq

type PublisherConfig struct {
	Url          string `mapstructure:"url" json:"url,omitempty" gorm:"column:url" bson:"url,omitempty" dynamodbav:"url,omitempty" firestore:"url,omitempty"`
	ExchangeName string `mapstructure:"exchange_name" json:"exchangeName,omitempty" gorm:"column:exchangename" bson:"exchangeName,omitempty" dynamodbav:"exchangeName,omitempty" firestore:"exchangeName,omitempty"`
	ExchangeKind string `mapstructure:"exchange_kind" json:"exchangeKind,omitempty" gorm:"column:exchangekind" bson:"exchangeKind,omitempty" dynamodbav:"exchangeKind,omitempty" firestore:"exchangeKind,omitempty"`
	Key          string `mapstructure:"key" json:"key,omitempty" gorm:"column:key" bson:"key,omitempty" dynamodbav:"key,omitempty" firestore:"key,omitempty"`
	AutoDelete   bool   `mapstructure:"auto_delete" json:"autoDelete,omitempty" gorm:"column:autodelete" bson:"autoDelete,omitempty" dynamodbav:"autoDelete,omitempty" firestore:"autoDelete,omitempty"`
	ContentType  string `mapstructure:"content_type" json:"contentType,omitempty" gorm:"column:contentType" bson:"contentType,omitempty" dynamodbav:"contentType,omitempty" firestore:"contentType,omitempty"`
}
