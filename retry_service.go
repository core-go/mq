package mq

import "context"

type RetryService interface {
	Retry(ctx context.Context, message *Message) error
}
