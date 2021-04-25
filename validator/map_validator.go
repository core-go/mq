package validator

import "context"

type MapValidator interface {
	Validate(ctx context.Context, model map[string]interface{}) ([]ErrorMessage, error)
}
