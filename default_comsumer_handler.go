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
type DefaultConsumerHandler struct {
	ModelType      reflect.Type
	Validate       func(ctx context.Context, message *Message) error
	Write          func(ctx context.Context, model interface{}) error
	LimitRetry     int
	Retry          func(ctx context.Context, message *Message) error
	RetryCountName string
	Error          func(ctx context.Context, message *Message) error
	Retries        []time.Duration
	Goroutines     bool
	LogError       func(context.Context, string)
	LogInfo        func(context.Context, string)
}

func NewConsumerHandlerByConfig(c ConsumerConfig, modelType reflect.Type, write func(context.Context, interface{}) error, retry func(context.Context, *Message) error, validate func(context.Context, *Message) error, handleError func(context.Context, *Message) error, logs ...func(context.Context, string)) *DefaultConsumerHandler {
	return NewConsumerHandlerWithRetryService(modelType, write, c.LimitRetry, retry, c.RetryCountName, validate, handleError, c.Goroutines, logs...)
}
func NewConsumerHandlerWithRetryConfig(modelType reflect.Type, write func(context.Context, interface{}) error, validate func(context.Context, *Message) error, c *RetryConfig, goroutines bool, logs ...func(context.Context, string)) *DefaultConsumerHandler {
	if c == nil {
		return NewConsumerHandlerWithRetries(modelType, write, validate, nil, goroutines, logs...)
	}
	retries := DurationsFromValue(*c, "Retry", 20)
	if len(retries) == 0 {
		return NewConsumerHandlerWithRetries(modelType, write, validate, nil, goroutines, logs...)
	}
	return NewConsumerHandlerWithRetries(modelType, write, validate, retries, goroutines, logs...)
}
func NewConsumerHandlerWithRetries(modelType reflect.Type, write func(context.Context, interface{}) error, validate func(context.Context, *Message) error, retries []time.Duration, goroutines bool, logs ...func(context.Context, string)) *DefaultConsumerHandler {
	c := &DefaultConsumerHandler{
		ModelType:  modelType,
		Write:      write,
		Validate:   validate,
		Goroutines: goroutines,
	}
	if retries != nil {
		c.Retries = retries
	}
	if len(logs) >= 1 {
		c.LogError = logs[0]
	}
	if len(logs) >= 2 {
		c.LogInfo = logs[1]
	}
	return c
}
func NewConsumerHandler(modelType reflect.Type, write func(context.Context, interface{}) error, validate func(context.Context, *Message) error, goroutines bool, logs ...func(context.Context, string)) *DefaultConsumerHandler {
	return NewConsumerHandlerWithRetryService(modelType, write, -1, nil, "", validate, nil, goroutines, logs...)
}
func NewConsumerHandlerWithRetryService(modelType reflect.Type, write func(context.Context, interface{}) error, limitRetry int, retry func(context.Context, *Message) error, retryCountName string, validate func(context.Context, *Message) error,
	handleError func(context.Context, *Message) error,
	goroutines bool, logs ...func(context.Context, string)) *DefaultConsumerHandler {
	if len(retryCountName) == 0 {
		retryCountName = "retryCount"
	}
	if retry != nil && handleError == nil {
		e1 := NewErrorHandler(logs...)
		handleError = e1.HandleError
	}
	c := &DefaultConsumerHandler{
		ModelType:      modelType,
		Write:          write,
		Validate:       validate,
		LimitRetry:     limitRetry,
		Retry:          retry,
		RetryCountName: retryCountName,
		Error:          handleError,
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

func (c *DefaultConsumerHandler) Handle(ctx context.Context, message *Message, err error) error {
	if err != nil {
		if c.LogError != nil {
			c.LogError(ctx, "Processing message error: "+err.Error())
		}
		return err
	} else if message == nil {
		return nil
	}
	if c.LogInfo != nil {
		c.LogInfo(ctx, fmt.Sprintf("Received message: %s", message.Data))
	}
	if c.Validate != nil {
		er2 := c.Validate(ctx, message)
		if er2 != nil {
			if c.LogError != nil {
				l := logMessage{Id: message.Id, Data: message.Data, Attributes: message.Attributes}
				c.LogError(ctx, fmt.Sprintf("Message is invalid: %s . Error: %s", l, er2.Error()))
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
				c.LogError(ctx, fmt.Sprintf(`cannot unmarshal item: %s. Error: %s`, message.Data, er1.Error()))
			}
			return nil
		}
		item = v
	}
	if c.Goroutines {
		go c.write(ctx, message, item)
		return nil
	} else {
		return c.write(ctx, message, item)
	}
}

func (c *DefaultConsumerHandler) write(ctx context.Context, message *Message, item interface{}) error {
	ctx = context.WithValue(ctx, "message", message)
	if c.Retry == nil && c.Retries != nil && len(c.Retries) > 0 {
		if er1 := c.Write(ctx, item); er1 != nil {
			i := 0
			err := Retry(ctx, c.Retries, func() (err error) {
				i = i + 1
				er2 := c.Write(ctx, item)
				if er2 == nil && c.LogError != nil {
					c.LogError(ctx, fmt.Sprintf("Write successfully after %d retries %s", i, message.Data))
				}
				return er2
			}, c.LogError)
			if err != nil && c.LogError != nil {
				c.LogError(ctx, fmt.Sprintf("Failed to write after %d retries: %s. Error: %s.", len(c.Retries), message.Data, er1.Error()))
			}
			return err
		}
		return nil
	} else {
		er3 := c.Write(ctx, item)
		if er3 == nil {
			return er3
		}
		c.LogError(ctx, fmt.Sprintf("Fail to write %s . Error: %s", message.Data, er3.Error()))
		if c.Retry == nil {
			if c.Error != nil {
				c.Error(ctx, message)
			}
			return er3
		}
		retryCount := 0
		if message.Attributes == nil {
			message.Attributes = make(map[string]string)
		} else {
			var er4 error
			retryCount, er4 = strconv.Atoi(message.Attributes[c.RetryCountName])
			if er4 != nil {
				retryCount = 0
			}
		}
		retryCount++
		if retryCount > c.LimitRetry {
			if c.LogInfo != nil {
				l := logMessage{Id: message.Id, Data: message.Data, Attributes: message.Attributes}
				c.LogInfo(ctx, fmt.Sprintf("Retry: %d . Retry limitation: %d . Message: %s.", retryCount, c.LimitRetry, l))
			}
			if c.Error != nil {
				c.Error(ctx, message)
			}
		} else {
			if c.LogInfo != nil {
				l := logMessage{Id: message.Id, Data: message.Data, Attributes: message.Attributes}
				c.LogInfo(ctx, fmt.Sprintf("Retry: %d . Message: %s.", retryCount, l))
			}
			message.Attributes[c.RetryCountName] = strconv.Itoa(retryCount)
			er2 := c.Retry(ctx, message)
			if er2 != nil {
				if c.LogError != nil {
					c.LogError(ctx, fmt.Sprintf("Cannot retry %s . Error: %s", message, er2.Error()))
				}
			}
		}
		return nil
	}
}
