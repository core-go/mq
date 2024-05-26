package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Handler[T any] struct {
	Write       func(ctx context.Context, model *T) error
	Validate    func(context.Context, *T) ([]ErrorMessage, error)
	HandleError func(context.Context, *T, []ErrorMessage, []byte)
	Retries     []time.Duration
	Goroutines  bool
	LogError    func(context.Context, string)
	LogInfo     func(context.Context, string)
	Key         string
}

func NewHandlerByConfig[T any](c *RetryConfig, write func(context.Context, *T) error, validate func(context.Context, *T) ([]ErrorMessage, error), handleError func(context.Context, *T, []ErrorMessage, []byte), goroutines bool, key string, logs ...func(context.Context, string)) *Handler[T] {
	if c == nil {
		return NewHandlerWithKey[T](write, validate, handleError, nil, goroutines, key, logs...)
	} else {
		retries := DurationsFromValue(*c, "Retry", 20)
		return NewHandlerWithKey[T](write, validate, handleError, retries, goroutines, key, logs...)
	}
}
func NewHandlerWithKey[T any](write func(context.Context, *T) error, validate func(context.Context, *T) ([]ErrorMessage, error), handleError func(context.Context, *T, []ErrorMessage, []byte), retries []time.Duration, goroutines bool, key string, logs ...func(context.Context, string)) *Handler[T] {
	c := &Handler[T]{
		Write:       write,
		Validate:    validate,
		HandleError: handleError,
		Retries:     retries,
		Goroutines:  goroutines,
		Key:         key,
	}
	if len(logs) >= 1 {
		c.LogError = logs[0]
	}
	if len(logs) >= 2 {
		c.LogInfo = logs[1]
	}
	return c
}

func (c *Handler[T]) Handle(ctx context.Context, data []byte) {
	if data == nil {
		return
	}
	if c.LogInfo != nil {
		key := GetString(ctx, c.Key)
		if len(key) > 0 {
			c.LogInfo(ctx, fmt.Sprintf("Received message with key %s : %s", key, data))
		} else {
			c.LogInfo(ctx, fmt.Sprintf("Received message: %s", data))
		}
	}
	var v T
	er1 := json.Unmarshal(data, &v)
	if er1 != nil {
		if c.LogError != nil {
			c.LogError(ctx, fmt.Sprintf("cannot unmarshal item: %s. Error: %s", data, er1.Error()))
		}
		return
	}
	if c.Validate != nil {
		errs, err := c.Validate(ctx, &v)
		if err != nil {
			if c.LogError != nil {
				c.LogError(ctx, "Error when validate data: "+err.Error())
			}
			return
		}
		if len(errs) > 0 {
			c.HandleError(ctx, &v, errs, data)
		}
	}
	if c.Goroutines {
		go c.write(ctx, data, &v)
	} else {
		c.write(ctx, data, &v)
	}
}

func (c *Handler[T]) write(ctx context.Context, data []byte, item *T) error {
	er3 := c.Write(ctx, item)
	if er3 == nil {
		return er3
	}
	if c.Retries != nil && len(c.Retries) > 0 {
		i := 0
		err := Retry(ctx, c.Retries, func() (err error) {
			i = i + 1
			er2 := c.Write(ctx, item)
			if er2 == nil {
				if c.LogError != nil {
					c.LogError(ctx, fmt.Sprintf("Write successfully after %d retries %s", i, data))
				}
			}
			return er2
		}, c.LogError)
		if err != nil && c.LogError != nil {
			c.LogError(ctx, fmt.Sprintf("Failed to write after %d retries: %s. Error: %s.", len(c.Retries), data, err.Error()))
		}
		return nil
	} else {
		if c.LogError != nil {
			c.LogError(ctx, fmt.Sprintf("Failed to write %s . Error: %s", data, er3.Error()))
		}
		return er3
	}
}

// Retry Copy this code from https://stackoverflow.com/questions/47606761/repeat-code-if-an-error-occured
func Retry(ctx context.Context, sleeps []time.Duration, f func() error, log func(context.Context, string)) (err error) {
	attempts := len(sleeps)
	for i := 0; ; i++ {
		time.Sleep(sleeps[i])
		err = f()
		if err == nil {
			return
		}
		if i >= (attempts - 1) {
			break
		}
		if log != nil {
			log(ctx, fmt.Sprintf("Retrying %d of %d after error: %s", i+1, attempts, err.Error()))
		}
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}
