package mq

import (
	"context"
	"encoding/json"
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
}

func NewConsumerCallerByConfig(c ConsumerConfig, modelType reflect.Type, writer Writer, retryService RetryService, validator Validator, errorHandler ErrorHandler) *DefaultConsumerCaller {
	return NewConsumerCaller(modelType, writer, c.LimitRetry, retryService, c.RetryCountName, validator, errorHandler, c.Goroutines)
}
func NewConsumerCallerWithRetries(modelType reflect.Type, writer Writer, validator Validator, retries []time.Duration, errorHandler ErrorHandler, goroutines bool) *DefaultConsumerCaller {
	if errorHandler == nil {
		errorHandler = NewErrorHandler()
	}
	c := DefaultConsumerCaller{
		ModelType:    modelType,
		Writer:       writer,
		Validator:    validator,
		Retries:      &retries,
		ErrorHandler: errorHandler,
		Goroutines:   goroutines,
	}
	return &c
}
func NewConsumerCaller(modelType reflect.Type, writer Writer, limitRetry int, retryService RetryService, retryCountName string, validator Validator,
	errorHandler ErrorHandler,
	goroutines bool) *DefaultConsumerCaller {
	if len(retryCountName) == 0 {
		retryCountName = "retryCount"
	}
	if errorHandler == nil {
		errorHandler = NewErrorHandler()
	}
	c := DefaultConsumerCaller{
		ModelType:      modelType,
		Writer:         writer,
		Validator:      validator,
		LimitRetry:     limitRetry,
		RetryService:   retryService,
		RetryCountName: retryCountName,
		ErrorHandler:   errorHandler,
		Goroutines:     goroutines,
	}
	return &c
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
		Errorf(ctx, "Processing message error: %s", err.Error())
		return err
	} else if message == nil {
		return nil
	}
	if IsDebugEnabled() {
		Debugf(ctx, "Received message: %s", message.Data)
	}
	if c.Validator != nil {
		er2 := c.Validator.Validate(ctx, message)
		if er2 != nil {
			Errorf(ctx, "Message is invalid: %v  Error: %s", message, er2.Error())
			return er2
		}
	}

	item := message.Value
	if item == nil {
		v := InitModel(c.ModelType)
		er1 := json.Unmarshal(message.Data, v)
		if er1 != nil {
			Errorf(ctx, `can't unmarshal item: %v. Error: %s`, string(message.Data), er1.Error())
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
				if er2 == nil {
					s := string(message.Data)
					Infof(ctx, "Write successfully after %d retries %s", i, s)
				}
				return er2
			})
			if err != nil {
				s := string(message.Data)
				Errorf(ctx, "Failed to write: %s. Error: %v.", s, er1)
			}
			return err
		}
		return nil
	} else {
		er3 := c.Writer.Write(ctx, item)
		if er3 == nil {
			return er3
		}
		Error(ctx, er3.Error())
		if c.RetryService == nil {
			return er3
		}

		retryCount, err := strconv.Atoi(message.Attributes[c.RetryCountName])
		if err != nil {
			retryCount = 1
		}
		retryCount++
		if retryCount > c.LimitRetry {
			if IsInfoEnabled() {
				Infof(ctx, "Retry: %d . Retry limitation: %d . Message: %v.", retryCount, c.LimitRetry, message)
			}
			if c.ErrorHandler != nil {
				c.ErrorHandler.HandleError(ctx, message)
			}
		} else {
			if IsDebugEnabled() {
				Debugf(ctx, "Retry: %d . Message: %v.", retryCount, message)
			}
			message.Attributes[c.RetryCountName] = strconv.Itoa(retryCount)
			er2 := c.RetryService.Retry(ctx, message)
			if er2 != nil {
				Errorf(ctx, "Cannot retry %v . Error: %s", message, er2.Error())
			}
		}
		return nil
	}
}
