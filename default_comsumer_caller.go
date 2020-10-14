package mq

import (
	"context"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"reflect"
	"strconv"
)

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

func NewDefaultConsumerCaller(modelType reflect.Type, writer Writer, limitRetry int, retryService RetryService, retryCountName string, validator Validator,
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

	if message == nil {
		if logrus.IsLevelEnabled(logrus.DebugLevel) {
			logrus.Debug("OnConsume - message is nil")
		}
		return nil
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
