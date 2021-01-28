package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

type ConsumerConfig struct {
	RetryCountName string `mapstructure:"retry_count_name"`
	LimitRetry     int    `mapstructure:"limit_retry"`
	Goroutines     bool   `mapstructure:"goroutines"`
}
type DefaultConsumerCaller struct {
	ModelType      reflect.Type
	Validator      Validator
	Writer         Writer
	LimitRetry     int
	RetryService   RetryService
	RetryCountName string
	ErrorHandler   ErrorHandler
	Retries        *[]time.Duration
	Goroutines     bool
	LogError       func(context.Context, string)
	LogInfo        func(context.Context, string)
}

func NewConsumerCallerByConfig(c ConsumerConfig, modelType reflect.Type, writer Writer, retryService RetryService, validator Validator, errorHandler ErrorHandler, logs ...func(context.Context, string)) *DefaultConsumerCaller {
	return NewConsumerCallerWithRetryService(modelType, writer, c.LimitRetry, retryService, c.RetryCountName, validator, errorHandler, c.Goroutines, logs...)
}
func NewConsumerCallerWithRetries(modelType reflect.Type, writer Writer, validator Validator, retries []time.Duration, goroutines bool, logs ...func(context.Context, string)) *DefaultConsumerCaller {
	c := &DefaultConsumerCaller{
		ModelType:    modelType,
		Writer:       writer,
		Validator:    validator,
		Retries:      &retries,
		Goroutines:   goroutines,
	}
	if len(logs) >= 1 {
		c.LogError = logs[0]
	}
	if len(logs) >= 2 {
		c.LogInfo = logs[1]
	}
	return c
}
func NewConsumerCaller(modelType reflect.Type, writer Writer, validator Validator, goroutines bool, logs ...func(context.Context, string)) *DefaultConsumerCaller {
	return NewConsumerCallerWithRetryService(modelType, writer, -1, nil, "", validator, nil, goroutines, logs...)
}
func NewConsumerCallerWithRetryService(modelType reflect.Type, writer Writer, limitRetry int, retryService RetryService, retryCountName string, validator Validator,
	errorHandler ErrorHandler,
	goroutines bool, logs ...func(context.Context, string)) *DefaultConsumerCaller {
	if len(retryCountName) == 0 {
		retryCountName = "retryCount"
	}
	if retryService != nil && errorHandler == nil {
		errorHandler = NewErrorHandler()
	}
	c := &DefaultConsumerCaller{
		ModelType:      modelType,
		Writer:         writer,
		Validator:      validator,
		LimitRetry:     limitRetry,
		RetryService:   retryService,
		RetryCountName: retryCountName,
		ErrorHandler:   errorHandler,
		Goroutines:     goroutines,
	}
	if len(logs) >= 1 {
		c.LogError = logs[0]
	}
	if len(logs) >= 2 {
		c.LogInfo = logs[1]
	}
	return c
}

func MakeDurations(vs []int64) []time.Duration {
	durations := make([]time.Duration, 0)
	for _, v := range vs {
		d := time.Duration(v) * time.Second
		durations = append(durations, d)
	}
	return durations
}
func (c *DefaultConsumerCaller) Call(ctx context.Context, message *Message, err error) error {
	if err != nil {
		if c.LogError != nil {
			c.LogError(ctx, "Processing message error: " + err.Error())
		}
		return err
	} else if message == nil {
		return nil
	}
	if c.LogInfo != nil {
		c.LogInfo(ctx, "Received message: "+string(message.Data))
	}
	if c.Validator != nil {
		er2 := c.Validator.Validate(ctx, message)
		if er2 != nil {
			if c.LogError != nil {
				c.LogError(ctx, fmt.Sprintf("Message is invalid: %s  Error: %s", message, er2.Error()))
			}
			return er2
		}
	}

	item := message.Value
	if item == nil {
		v := InitModel(c.ModelType)
		er1 := json.Unmarshal(message.Data, v)
		if er1 != nil {
			if c.LogError != nil {
				c.LogError(ctx, fmt.Sprintf(`can't unmarshal item: %s. Error: %s`, string(message.Data), er1.Error()))
			}
			return nil
		}
		item = reflect.Indirect(reflect.ValueOf(v)).Interface()
	}
	if c.Goroutines {
		go c.write(ctx, message, item)
		return nil
	} else {
		return c.write(ctx, message, item)
	}
}

func (c *DefaultConsumerCaller) write(ctx context.Context, message *Message, item interface{}) error {
	ctx = context.WithValue(ctx, "message", message)
	if c.RetryService == nil && c.Retries != nil && len(*c.Retries) > 0 {
		if er1 := c.Writer.Write(ctx, item); er1 != nil {
			i := 0
			err := Retry(ctx, *c.Retries, func() (err error) {
				i = i + 1
				er2 := c.Writer.Write(ctx, item)
				if er2 == nil && c.LogError != nil {
					c.LogError(ctx, fmt.Sprintf("Write successfully after %d retries %s", i, string(message.Data)))
				}
				return er2
			}, c.LogError)
			if err != nil && c.LogError != nil {
				c.LogError(ctx, fmt.Sprintf("Failed to write: %s. Error: %s.", string(message.Data), er1.Error()))
			}
			return err
		}
		return nil
	} else {
		er3 := c.Writer.Write(ctx, item)
		if er3 == nil {
			return er3
		}
		c.LogError(ctx, er3.Error())
		if c.RetryService == nil {
			if c.ErrorHandler != nil {
				c.ErrorHandler.HandleError(ctx, message)
			}
			return er3
		}

		retryCount, err := strconv.Atoi(message.Attributes[c.RetryCountName])
		if err != nil {
			retryCount = 1
		}
		retryCount++
		if retryCount > c.LimitRetry {
			if c.LogInfo != nil {
				c.LogInfo(ctx, fmt.Sprintf("Retry: %d . Retry limitation: %d . Message: %s.", retryCount, c.LimitRetry, message))
			}
			if c.ErrorHandler != nil {
				c.ErrorHandler.HandleError(ctx, message)
			}
		} else {
			if c.LogInfo != nil {
				c.LogInfo(ctx, fmt.Sprintf("Retry: %d . Message: %s.", retryCount, message))
			}
			message.Attributes[c.RetryCountName] = strconv.Itoa(retryCount)
			er2 := c.RetryService.Retry(ctx, message)
			if er2 != nil {
				if c.LogError != nil {
					c.LogError(ctx, fmt.Sprintf("Cannot retry %s . Error: %s", message, er2.Error()))
				}
			}
		}
		return nil
	}
}
