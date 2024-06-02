package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type HandlerConfig struct {
	Retry      *RetryConfig `yaml:"retry" mapstructure:"retry" json:"retry,omitempty" gorm:"column:retry" bson:"retry,omitempty" dynamodbav:"retry,omitempty" firestore:"retry,omitempty"`
	Goroutines bool         `yaml:"goroutines" mapstructure:"goroutines" json:"goroutines,omitempty" gorm:"column:goroutines" bson:"goroutines,omitempty" dynamodbav:"goroutines,omitempty" firestore:"goroutines,omitempty"`
	Key        string       `yaml:"key" mapstructure:"key" json:"key,omitempty" gorm:"column:key" bson:"key,omitempty" dynamodbav:"key,omitempty" firestore:"key,omitempty"`
}
type Handler[T any] struct {
	Unmarshal   func(data []byte, v any) error
	Write       func(ctx context.Context, model *T) error
	Validate    func(context.Context, *T) ([]ErrorMessage, error)
	Reject      func(context.Context, *T, []ErrorMessage, []byte)
	HandleError func(context.Context, []byte)
	Retries     []time.Duration
	Goroutines  bool
	Key         string
	LogError    func(context.Context, string)
	LogInfo     func(context.Context, string)
}

func NewHandlerByConfig[T any](c HandlerConfig,
	write func(context.Context, *T) error,
	validate func(context.Context, *T) ([]ErrorMessage, error),
	reject func(context.Context, *T, []ErrorMessage, []byte),
	handleError func(context.Context, []byte),
	goroutines bool, key string, logs ...func(context.Context, string)) *Handler[T] {
	return NewHandlerByConfigAndUnmarshal[T](c, nil, write, validate, reject, handleError, goroutines, key, logs...)
}
func NewHandlerByConfigAndUnmarshal[T any](c HandlerConfig,
	unmarshal func(data []byte, v any) error,
	write func(context.Context, *T) error,
	validate func(context.Context, *T) ([]ErrorMessage, error),
	reject func(context.Context, *T, []ErrorMessage, []byte),
	handleError func(context.Context, []byte),
	goroutines bool, key string, logs ...func(context.Context, string)) *Handler[T] {
	if c.Retry == nil {
		return NewHandlerWithKey[T](unmarshal, write, validate, reject, handleError, nil, c.Goroutines, c.Key, logs...)
	} else {
		retries := DurationsFromValue(c.Retry, "Retry", 20)
		return NewHandlerWithKey[T](unmarshal, write, validate, reject, handleError, retries, c.Goroutines, c.Key, logs...)
	}
}
func NewHandlerWithKey[T any](
	unmarshal func(data []byte, v any) error,
	write func(context.Context, *T) error,
	validate func(context.Context, *T) ([]ErrorMessage, error),
	reject func(context.Context, *T, []ErrorMessage, []byte),
	handleError func(context.Context, []byte),
	retries []time.Duration,
	goroutines bool, key string, logs ...func(context.Context, string)) *Handler[T] {
	if unmarshal == nil {
		unmarshal = json.Unmarshal
	}
	c := &Handler[T]{
		Write:       write,
		Unmarshal:   unmarshal,
		Validate:    validate,
		Reject:      reject,
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
			c.Reject(ctx, &v, errs, data)
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
			if c.HandleError != nil {
				c.HandleError(ctx, data)
			}
		}
		return nil
	} else {
		if c.LogError != nil {
			c.LogError(ctx, fmt.Sprintf("Failed to write %s . Error: %s", data, er3.Error()))
		}
		if c.HandleError != nil {
			c.HandleError(ctx, data)
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
