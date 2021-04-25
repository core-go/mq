package validator

import "context"

type Validator interface {
	Validate(ctx context.Context, model interface{}) ([]ErrorMessage, error)
}
