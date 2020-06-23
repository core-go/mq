package mq

import "context"

type Validator interface {
	Validate(ctx context.Context, message *Message) error
}
