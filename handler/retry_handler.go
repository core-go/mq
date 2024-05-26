package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
)

type HandlerConfig struct {
	RetryCountName string `yaml:"retry_count_name" mapstructure:"retry_count_name" json:"retryCountName,omitempty" gorm:"column:retrycountname" bson:"retryCountName,omitempty" dynamodbav:"retryCountName,omitempty" firestore:"retryCountName,omitempty"`
	LimitRetry     int    `yaml:"limit_retry" mapstructure:"limit_retry" json:"limitRetry,omitempty" gorm:"column:limitretry" bson:"limitRetry,omitempty" dynamodbav:"limitRetry,omitempty" firestore:"limitRetry,omitempty"`
	Goroutines     bool   `yaml:"goroutines" mapstructure:"goroutines" json:"goroutines,omitempty" gorm:"column:goroutines" bson:"goroutines,omitempty" dynamodbav:"goroutines,omitempty" firestore:"goroutines,omitempty"`
	Key            string `yaml:"key" mapstructure:"key" json:"key,omitempty" gorm:"column:key" bson:"key,omitempty" dynamodbav:"key,omitempty" firestore:"key,omitempty"`
}

type RetryHandler[T any] struct {
	Write           func(context.Context, *T) error
	Validate        func(context.Context, *T) ([]ErrorMessage, error)
	HandleError     func(context.Context, *T, []ErrorMessage, []byte, map[string]string)
	LimitRetry      int
	Retry           func(context.Context, []byte, map[string]string) error
	RetryCountName  string
	HandleException func(context.Context, []byte, map[string]string)
	Goroutines      bool
	LogError        func(context.Context, string)
	LogInfo         func(context.Context, string)
	Key             string
}

func NewRetryHandler[T any](write func(context.Context, *T) error, validate func(context.Context, *T) ([]ErrorMessage, error), handleError func(context.Context, *T, []ErrorMessage, []byte, map[string]string), goroutines bool, key string, logs ...func(context.Context, string)) *RetryHandler[T] {
	c := &RetryHandler[T]{
		Write:       write,
		Validate:    validate,
		HandleError: handleError,
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

func (c *RetryHandler[T]) Handle(ctx context.Context, data []byte, attrs map[string]string) {
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
			c.HandleError(ctx, &v, errs, data, attrs)
		}
	}
	if c.Goroutines {
		go Write[*T](ctx, c.Write, &v, data, attrs, c.HandleException, c.Retry, c.LimitRetry, c.RetryCountName, c.LogError, c.LogInfo)
	} else {
		Write[*T](ctx, c.Write, &v, data, attrs, c.HandleException, c.Retry, c.LimitRetry, c.RetryCountName, c.LogError, c.LogInfo)
	}
}

func Write[T any](ctx context.Context, write func(context.Context, T) error, item T, data []byte, attrs map[string]string, handleError func(context.Context, []byte, map[string]string), retry func(context.Context, []byte, map[string]string) error, limitRetry int, retryCountName string, logs ...func(context.Context, string)) {
	var logError func(context.Context, string)
	var logInfo func(context.Context, string)
	if len(logs) > 0 {
		logError = logs[0]
	}
	if len(logs) > 1 {
		logInfo = logs[1]
	}
	er3 := write(ctx, item)
	if er3 == nil {
		return
	}
	if logError != nil {
		logError(ctx, fmt.Sprintf("Fail to write %s . Error: %s", data, er3.Error()))
	}

	if retry == nil {
		if handleError != nil {
			handleError(ctx, data, attrs)
		}
		return
	}
	retryCount := 0
	if attrs == nil {
		attrs = make(map[string]string)
	} else {
		var er4 error
		retryCount, er4 = strconv.Atoi(attrs[retryCountName])
		if er4 != nil {
			retryCount = 0
		}
	}
	retryCount++
	if retryCount > limitRetry {
		if logInfo != nil {
			if attrs == nil || len(attrs) == 0 {
				logInfo(ctx, fmt.Sprintf("Retry: %d . Retry limitation: %d . Message: %s.", retryCount, limitRetry, data))
			} else {
				logInfo(ctx, fmt.Sprintf("Retry: %d . Retry limitation: %d . Message: %s %s.", retryCount, limitRetry, data, attrs))
			}

		}
		if handleError != nil {
			handleError(ctx, data, attrs)
		}
	} else {
		if logInfo != nil {
			if attrs == nil || len(attrs) == 0 {
				logInfo(ctx, fmt.Sprintf("Retry: %d . Message: %s.", retryCount, data))
			} else {
				logInfo(ctx, fmt.Sprintf("Retry: %d . Message: %s %s.", retryCount, data, attrs))
			}
		}
		attrs[retryCountName] = strconv.Itoa(retryCount)
		er2 := retry(ctx, data, attrs)
		if er2 != nil {
			if logError != nil {
				if attrs == nil || len(attrs) == 0 {
					logError(ctx, fmt.Sprintf("Cannot retry %s . Error: %s", data, er2.Error()))
				} else {
					logError(ctx, fmt.Sprintf("Cannot retry %s %s. Error: %s", data, attrs, er2.Error()))
				}
			}
		}
	}
}
