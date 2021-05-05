package pubsub

type ClientConfig struct {
	ProjectId   string `mapstructure:"project_id" json:"projectId,omitempty" gorm:"column:projectid" bson:"projectId,omitempty" dynamodbav:"projectId,omitempty" firestore:"projectId,omitempty"`
	Credentials string `mapstructure:"credentials" json:"credentials,omitempty" gorm:"column:credentials" bson:"credentials,omitempty" dynamodbav:"credentials,omitempty" firestore:"credentials,omitempty"`
	KeyFilename string `mapstructure:"key_filename" json:"keyFilename,omitempty" gorm:"column:keyfilename" bson:"keyFilename,omitempty" dynamodbav:"keyFilename,omitempty" firestore:"keyFilename,omitempty"`
}
