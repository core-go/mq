package activemq

type Config struct {
	Addr             string `mapstructure:"addr" json:"addr,omitempty" gorm:"column:addr" bson:"addr,omitempty" dynamodbav:"addr,omitempty" firestore:"addr,omitempty"`
	Host             string `mapstructure:"host" json:"host,omitempty" gorm:"column:host" bson:"host,omitempty" dynamodbav:"host,omitempty" firestore:"host,omitempty"`
	UserName         string `mapstructure:"username" json:"username,omitempty" gorm:"column:username" bson:"username,omitempty" dynamodbav:"username,omitempty" firestore:"username,omitempty"`
	Password         string `mapstructure:"password" json:"password,omitempty" gorm:"column:password" bson:"password,omitempty" dynamodbav:"password,omitempty" firestore:"password,omitempty"`
	DestinationName  string `mapstructure:"destination_name" json:"destinationName,omitempty" gorm:"column:destinationname" bson:"destinationName,omitempty" dynamodbav:"destinationName,omitempty" firestore:"destinationName,omitempty"`
	SubscriptionName string `mapstructure:"subscription_name" json:"subscriptionName,omitempty" gorm:"column:subscriptionname" bson:"subscriptionName,omitempty" dynamodbav:"subscriptionName,omitempty" firestore:"subscriptionName,omitempty"`
}
