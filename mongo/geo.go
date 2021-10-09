package mongo

type JSON struct {
	Type        string    `json:"type,omitempty" bson:"type,omitempty"`
	Coordinates []float64 `json:"coordinates,omitempty" bson:"coordinates,omitempty"`
}
