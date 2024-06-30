package nats

type SubscriberConfig struct {
	Subject    string     `mapstructure:"subject" json:"subject,omitempty" gorm:"column:subject" bson:"subject,omitempty" dynamodbav:"subject,omitempty" firestore:"subject,omitempty"`
	Connection ConnConfig `mapstructure:"connection" json:"connection,omitempty" gorm:"column:connection" bson:"connection,omitempty" dynamodbav:"connection,omitempty" firestore:"connection,omitempty"`
}
