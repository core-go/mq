package mq

import "context"

type MapsWriter interface {
	WriteBatch(ctx context.Context, models []map[string]interface{}) ([]int, []int, error) // Return: Success indices, Fail indices, Error
}
