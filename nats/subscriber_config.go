package nats

type SubscriberConfig struct {
	Subject    string     `mapstructure:"subject" json:"subject,omitempty" gorm:"column:subject" bson:"subject,omitempty" dynamodbav:"subject,omitempty" firestore:"subject,omitempty"`
	Header     bool       `mapstructure:"header" json:"header,omitempty" gorm:"column:header" bson:"header,omitempty" dynamodbav:"header,omitempty" firestore:"header,omitempty"`
	Connection ConnConfig `mapstructure:"connection" json:"connection,omitempty" gorm:"column:connection" bson:"connection,omitempty" dynamodbav:"connection,omitempty" firestore:"connection,omitempty"`
}
