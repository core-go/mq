package mq

import (
	"context"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"reflect"
	"strconv"
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
	Goroutines     bool
}
func NewConsumerCallerByConfig(c ConsumerConfig, modelType reflect.Type, writer Writer, retryService RetryService, validator Validator, errorHandler ErrorHandler) *DefaultConsumerCaller {
	return NewConsumerCaller(modelType, writer, c.LimitRetry, retryService, c.RetryCountName, validator, errorHandler, c.Goroutines)
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
	return &DefaultConsumerCaller{
		ModelType:      modelType,
		Writer:         writer,
		Validator:      validator,
		LimitRetry:     limitRetry,
		RetryService:   retryService,
		RetryCountName: retryCountName,
		ErrorHandler:   errorHandler,
		Goroutines:     goroutines,
	}
}

func (c *DefaultConsumerCaller) Call(ctx context.Context, message *Message, err error) error {
	if err != nil {
		logrus.Errorf("Processing message error: %s", err.Error())
		return err
	} else if message == nil {
		if logrus.IsLevelEnabled(logrus.WarnLevel) {
			logrus.Warn("Do not proceed empty message")
		}
		return nil
	}

	if c.Validator != nil {
		er2 := c.Validator.Validate(ctx, message)
		if er2 != nil {
			logrus.Errorf("Message is invalid: %v  Error: %s", message, er2.Error())
			return er2
		}
	}

	item := message.Value
	if item == nil {
		v := InitModel(c.ModelType)
		er1 := json.Unmarshal(message.Data, v)
		if er1 != nil {
			logrus.Errorf(`can't unmarshal item: %v. Error: %s`, string(message.Data), er1.Error())
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
	er3 := c.Writer.Write(ctx, item)
	if er3 == nil {
		return er3
	}
	logrus.Error(er3.Error())
	if c.RetryService == nil {
		return er3
	}

	retryCount, err := strconv.Atoi(message.Attributes[c.RetryCountName])
	if err != nil {
		retryCount = 1
	}
	retryCount++
	if retryCount > c.LimitRetry {
		if logrus.IsLevelEnabled(logrus.InfoLevel) {
			logrus.Infof("Retry: %d . Retry limitation: %d . Message: %v.", retryCount, c.LimitRetry, message)
		}
		if c.ErrorHandler != nil {
			c.ErrorHandler.HandleError(ctx, message)
		}
	} else {
		if logrus.IsLevelEnabled(logrus.DebugLevel) {
			logrus.Debugf("Retry: %d . Message: %v.", retryCount, message)
		}
		message.Attributes[c.RetryCountName] = strconv.Itoa(retryCount)
		er2 := c.RetryService.Retry(ctx, message)
		if er2 != nil {
			logrus.Errorf("Cannot retry %v . Error: %s", message, er2.Error())
		}
	}
	return nil
}
