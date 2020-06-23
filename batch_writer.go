package mq

import "context"

type BatchWriter interface {
	WriteBatch(ctx context.Context, models interface{}) ([]int, []int, error) // Return: Success indices, Fail indices, Error
}
