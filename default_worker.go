package mq

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"

	"github.com/sirupsen/logrus"
)

type DefaultWorker struct {
	ModelType      reflect.Type
	Validator      Validator
	Writer         Writer
	LimitRetry     int
	RetryService   RetryService
	RetryCountName string
	ErrorHandler   ErrorHandler
	Goroutines     bool
}

func NewWorker(modelType reflect.Type, writer Writer, validator Validator, limitRetry int, retryService RetryService, retryCountName string, errorHandler ErrorHandler, goroutines bool) *DefaultWorker {
	if len(retryCountName) == 0 {
		retryCountName = "retryCount"
	}
	if errorHandler == nil {
		errorHandler = NewErrorHandler()
	}
	return &DefaultWorker{
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

func (w *DefaultWorker) OnConsume(ctx context.Context, msg *Message) {
	if msg == nil {
		if logrus.IsLevelEnabled(logrus.DebugLevel) {
			logrus.Debug("OnConsume - message is nil")
		}
		return
	}
	if w.Validator != nil {
		er2 := w.Validator.Validate(ctx, msg)
		if er2 != nil {
			logrus.Errorf("Message is invalid: %v  Error: %s", msg, er2.Error())
			return
		}
	}
	item := msg.Value
	if item == nil {
		v := InitModel(w.ModelType)
		er1 := json.Unmarshal(msg.Data, v)
		if er1 != nil {
			logrus.Errorf(`can't unmarshal item: %v. Error: %s`, string(msg.Data), er1.Error())
			return
		}
		item = reflect.Indirect(reflect.ValueOf(v)).Interface()
	}

	er3 := w.Writer.Write(ctx, item)
	if er3 == nil {
		return
	}
	logrus.Error(er3.Error())
	if w.RetryService == nil {
		return
	}

	retryCount, err := strconv.Atoi(msg.Attributes[w.RetryCountName])
	if err != nil {
		retryCount = 1
	}
	retryCount++
	if retryCount > w.LimitRetry {
		if logrus.IsLevelEnabled(logrus.InfoLevel) {
			logrus.Infof("Retry: %d . Retry limitation: %d . Message: %v.", retryCount, w.LimitRetry, msg)
		}
		if w.ErrorHandler != nil {
			w.ErrorHandler.HandleError(ctx, msg)
		}
	} else {
		if logrus.IsLevelEnabled(logrus.DebugLevel) {
			logrus.Debugf("Retry: %d . Message: %v.", retryCount, msg)
		}
		msg.Attributes[w.RetryCountName] = strconv.Itoa(retryCount)
		er2 := w.RetryService.Retry(ctx, msg)
		if er2 != nil {
			logrus.Errorf("Cannot retry %v . Error: %s", msg, er2.Error())
		}
	}
}
