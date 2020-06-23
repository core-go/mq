package mq

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type DefaultBatchWorker struct {
	batchSize          int
	timeout            int64
	limitRetry         int
	BatchHandler       BatchHandler
	RetryService       RetryService
	RetryCountName     string
	ErrorHandler       ErrorHandler
	messages           []*Message
	latestExecutedTime int64
	mux                sync.Mutex
}

func NewBatchWorkerByConfig(batchConfig BatchWorkerConfig, repository BatchHandler, retryService RetryService, retryCountName string, errorHandler ErrorHandler) *DefaultBatchWorker {
	return NewBatchWorker(batchConfig.BatchSize, batchConfig.Timeout, batchConfig.LimitRetry, repository, retryService, retryCountName, errorHandler)
}

func NewDefaultBatchWorker(batchConfig BatchWorkerConfig, repository BatchHandler, retryService RetryService) *DefaultBatchWorker {
	return NewBatchWorker(batchConfig.BatchSize, batchConfig.Timeout, batchConfig.LimitRetry, repository, retryService, "", nil)
}

func NewBatchWorker(batchSize int, timeout int64, limitRetry int, repository BatchHandler, retryService RetryService, retryCountName string, errorHandler ErrorHandler) *DefaultBatchWorker {
	if len(retryCountName) == 0 {
		retryCountName = "retryCount"
	}
	if errorHandler == nil {
		errorHandler = NewErrorHandler()
	}
	return &DefaultBatchWorker{
		batchSize:      batchSize,
		timeout:        timeout,
		limitRetry:     limitRetry,
		BatchHandler:   repository,
		RetryService:   retryService,
		RetryCountName: retryCountName,
		ErrorHandler:   errorHandler,
	}
}

func (w *DefaultBatchWorker) OnConsume(ctx context.Context, message *Message) {
	w.mux.Lock()

	if message != nil {
		w.messages = append(w.messages, message)
	} else if logrus.IsLevelEnabled(logrus.DebugLevel) {
		logrus.Debug("OnConsume - message is nil")
	}

	if w.readyToExecute(ctx) {
		w.execute(ctx)
	}
	w.mux.Unlock()
}

func (w *DefaultBatchWorker) readyToExecute(ctx context.Context) bool {
	isReady := false
	now := time.Now().Unix()
	batchSize := len(w.messages)

	if batchSize > 0 && (batchSize >= w.batchSize || w.latestExecutedTime+w.timeout < now) {
		isReady = true
	}
	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		if isReady {
			logrus.Debugf("Ready to execute: Next %d - Now %d - Batch Size %d - Size: %v - LatestExecutedTime: %v - Timeout: %v", w.latestExecutedTime+w.timeout, now, batchSize, w.batchSize, w.latestExecutedTime, w.timeout)
		} else {
			logrus.Debugf("Not ready to execute: Now: %v - Batch Size: %v - Size: %v - LatestExecutedTime: %v, Timeout: %v", now, batchSize, w.batchSize, w.latestExecutedTime, w.timeout)
		}
	}
	return isReady
}

func (w *DefaultBatchWorker) execute(ctx context.Context) {
	lenMessages := len(w.messages)
	if lenMessages == 0 {
		w.resetState(ctx)
		return
	}

	errList, err := w.BatchHandler.Handle(ctx, w.messages)

	if err != nil {
		logrus.Errorf("Error of Batch handling: %v", err)
	}
	if errList != nil && len(errList) > 0 {
		l := len(errList)
		for i := 0; i < l; i++ {
			if errList[i].Attributes == nil {
				errList[i].Attributes = map[string]string{}
			}
			retryCount, err := strconv.Atoi(errList[i].Attributes[w.RetryCountName])
			if err != nil {
				retryCount = 1
			}
			retryCount++

			if retryCount > w.limitRetry {
				if logrus.IsLevelEnabled(logrus.InfoLevel) {
					logrus.Infof("Retry: %d . Retry limitation: %d . Message: %v.", retryCount, w.limitRetry, errList[i])
				}
				w.ErrorHandler.HandleError(ctx, errList[i])
				break
			} else if logrus.IsLevelEnabled(logrus.DebugLevel) {
				logrus.Debugf("Retry: %d . Message: %v.", retryCount, errList[i])
			}

			errList[i].Attributes[w.RetryCountName] = strconv.Itoa(retryCount)
			er2 := w.RetryService.Retry(ctx, errList[i])
			if er2 != nil {
				logrus.Errorf("Cannot retry %v . Error: %s", errList[i], er2.Error())
			}
			break
		}
	}
	w.resetState(ctx)
}

func (w *DefaultBatchWorker) resetState(ctx context.Context) {
	w.messages = w.messages[:0]
	w.latestExecutedTime = time.Now().Unix()
}

func (w *DefaultBatchWorker) RunScheduler(ctx context.Context) {
	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		logrus.Debug("Enter DefaultBatchWorker.RunScheduler")
	}
	ticker := time.NewTicker(time.Duration(w.timeout) * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				w.OnConsume(ctx, nil)
				// do stuff
			}
		}
	}()
}
