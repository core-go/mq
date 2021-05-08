package health

type Health struct {
	Status  string                 `mapstructure:"status" json:"status,omitempty" gorm:"column:status" bson:"status,omitempty" dynamodbav:"status,omitempty" firestore:"status,omitempty"`
	Data    map[string]interface{} `mapstructure:"data" json:"data,omitempty" gorm:"column:data" bson:"data,omitempty" dynamodbav:"data,omitempty" firestore:"data,omitempty"`
	Details map[string]Health      `mapstructure:"details" json:"details,omitempty" gorm:"column:details" bson:"details,omitempty" dynamodbav:"details,omitempty" firestore:"details,omitempty"`
}
