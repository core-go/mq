package mq

import "context"

type Message[T any] struct {
	Data       []byte            `yaml:"data" mapstructure:"data" json:"data,omitempty" gorm:"column:data" bson:"data,omitempty" dynamodbav:"data,omitempty" firestore:"data,omitempty"`
	Attributes map[string]string `yaml:"attributes" mapstructure:"attributes" json:"attributes,omitempty" gorm:"column:attributes" bson:"attributes,omitempty" dynamodbav:"attributes,omitempty" firestore:"attributes,omitempty"`
	Value      T                 `yaml:"value" mapstructure:"value" json:"value,omitempty" gorm:"column:value" bson:"value,omitempty" dynamodbav:"value,omitempty" firestore:"value,omitempty"`
}

type BatchHandler[T any] struct {
	Write func(context.Context, []T) ([]int, error) // Return: Fail indices, Error
}

func NewBatchHandler[T any](writeBatch func(context.Context, []T) ([]int, error)) *BatchHandler[T] {
	h := &BatchHandler[T]{Write: writeBatch}
	return h
}

func (h *BatchHandler[T]) Handle(ctx context.Context, data []Message[T]) ([]Message[T], error) {
	failMessages := make([]Message[T], 0)
	var vs []T
	le := len(data)
	if le > 0 {
		for i := 0; i < le; i++ {
			message := data[i]
			vs = append(vs, message.Value)
		}
		failIndices, err := h.Write(ctx, vs)
		if err != nil {
			return failMessages, err
		}
		sl := len(failIndices)
		if sl > 0 {
			for j := 0; j < sl; j++ {
				failMessages = append(failMessages, data[failIndices[j]])
			}
		}
	}
	return failMessages, nil
}
