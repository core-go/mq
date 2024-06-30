package activemq

type Config struct {
	Addr             string `yaml:"addr" mapstructure:"addr" json:"addr,omitempty" gorm:"column:addr" bson:"addr,omitempty" dynamodbav:"addr,omitempty" firestore:"addr,omitempty"`
	Host             string `yaml:"host" mapstructure:"host" json:"host,omitempty" gorm:"column:host" bson:"host,omitempty" dynamodbav:"host,omitempty" firestore:"host,omitempty"`
	UserName         string `yaml:"username" mapstructure:"username" json:"username,omitempty" gorm:"column:username" bson:"username,omitempty" dynamodbav:"username,omitempty" firestore:"username,omitempty"`
	Password         string `yaml:"password" mapstructure:"password" json:"password,omitempty" gorm:"column:password" bson:"password,omitempty" dynamodbav:"password,omitempty" firestore:"password,omitempty"`
	DestinationName  string `yaml:"destination_name" mapstructure:"destination_name" json:"destinationName,omitempty" gorm:"column:destinationname" bson:"destinationName,omitempty" dynamodbav:"destinationName,omitempty" firestore:"destinationName,omitempty"`
	SubscriptionName string `yaml:"subscription_name" mapstructure:"subscription_name" json:"subscriptionName,omitempty" gorm:"column:subscriptionname" bson:"subscriptionName,omitempty" dynamodbav:"subscriptionName,omitempty" firestore:"subscriptionName,omitempty"`
}
