package mq

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"
)

const TimeFormat = "15:04:05.000"

type DefaultBatchWorker struct {
	batchSize          int
	timeout            int64
	limitRetry         int
	BatchHandler       BatchHandler
	RetryService       RetryService
	RetryCountName     string
	ErrorHandler       ErrorHandler
	Goroutine          bool
	messages           []*Message
	latestExecutedTime time.Time
	mux                sync.Mutex
	LogError           func(context.Context, string)
	LogInfo            func(context.Context, string)
}

func NewBatchWorkerByConfig(batchConfig BatchWorkerConfig, repository BatchHandler, retryService RetryService, retryCountName string, errorHandler ErrorHandler, logs ...func(context.Context, string)) *DefaultBatchWorker {
	return NewBatchWorker(batchConfig.BatchSize, batchConfig.Timeout, batchConfig.LimitRetry, repository, retryService, retryCountName, errorHandler, batchConfig.Goroutines, logs...)
}

func NewDefaultBatchWorker(batchConfig BatchWorkerConfig, repository BatchHandler, retryService RetryService, logs ...func(context.Context, string)) *DefaultBatchWorker {
	return NewBatchWorker(batchConfig.BatchSize, batchConfig.Timeout, batchConfig.LimitRetry, repository, retryService, "", nil, batchConfig.Goroutines, logs...)
}

func NewBatchWorker(batchSize int, timeout int64, limitRetry int, repository BatchHandler, retryService RetryService, retryCountName string, errorHandler ErrorHandler, goroutine bool, logs ...func(context.Context, string)) *DefaultBatchWorker {
	if len(retryCountName) == 0 {
		retryCountName = "retryCount"
	}
	if errorHandler == nil {
		errorHandler = NewErrorHandler()
	}
	w := &DefaultBatchWorker{
		batchSize:      batchSize,
		timeout:        timeout,
		limitRetry:     limitRetry,
		BatchHandler:   repository,
		RetryService:   retryService,
		RetryCountName: retryCountName,
		ErrorHandler:   errorHandler,
		Goroutine:      goroutine,
	}
	if len(logs) >= 1 {
		w.LogError = logs[0]
	}
	if len(logs) >= 2 {
		w.LogInfo = logs[1]
	}
	return w
}

func (w *DefaultBatchWorker) OnConsume(ctx context.Context, message *Message) {
	w.mux.Lock()
	if message != nil {
		w.messages = append(w.messages, message)
	}
	if w.ready(ctx) {
		w.execute(ctx)
	}
	w.mux.Unlock()
}

func (w *DefaultBatchWorker) ready(ctx context.Context) bool {
	isReady := false
	now := time.Now()
	timeoutStr := time.Duration(w.timeout) * time.Millisecond
	batchSize := len(w.messages)
	t := w.latestExecutedTime.Add(time.Duration(w.timeout) * time.Millisecond)
	if batchSize > 0 && (batchSize >= w.batchSize || t.Sub(now) < 0) {
		isReady = true
	}
	if isReady && w.LogInfo != nil {
		m := fmt.Sprintf("Run: %d / %v - Next %s - Last %s - Timeout: %v", batchSize, w.batchSize, t.Format(TimeFormat), w.latestExecutedTime.Format(TimeFormat), timeoutStr)
		w.LogInfo(ctx, m)
	}
	return isReady
}

func (w *DefaultBatchWorker) execute(ctx context.Context) {
	lenMessages := len(w.messages)
	if lenMessages == 0 {
		w.reset(ctx)
		return
	}

	errList, err := w.BatchHandler.Handle(ctx, w.messages)

	if err != nil && w.LogError != nil {
		m := "Error of Batch handling: " + err.Error()
		w.LogError(ctx, m)
	}
	if errList != nil && len(errList) > 0 {
		if w.RetryService == nil {
			if w.LogError != nil {
				l := len(errList)
				for i := 0; i < l; i++ {
					m := fmt.Sprintf("Error Message: %v.", errList[i])
					w.LogError(ctx, m)
				}
			}
		} else {
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
					if w.LogInfo != nil {
						m := fmt.Sprintf("Retry: %d . Retry limitation: %d . Message: %v.", retryCount, w.limitRetry, errList[i])
						w.LogInfo(ctx, m)
					}
					if w.ErrorHandler != nil {
						w.ErrorHandler.HandleError(ctx, errList[i])
					}
					continue
				} else {
					if w.LogInfo != nil {
						m := fmt.Sprintf("Retry: %d . Message: %v.", retryCount, errList[i])
						w.LogInfo(ctx, m)
					}
				}
				errList[i].Attributes[w.RetryCountName] = strconv.Itoa(retryCount)
				er2 := w.RetryService.Retry(ctx, errList[i])
				if er2 != nil && w.LogError != nil {
					m := fmt.Sprintf("Cannot retry %v . Error: %s", errList[i], er2.Error())
					w.LogError(ctx, m)
				}
			}
		}
	}
	w.reset(ctx)
}

func (w *DefaultBatchWorker) reset(ctx context.Context) {
	w.messages = w.messages[:0]
	w.latestExecutedTime = time.Now()
}

func (w *DefaultBatchWorker) RunScheduler(ctx context.Context) {
	w.reset(ctx)
	ticker := time.NewTicker(time.Duration(w.timeout) * time.Millisecond)
	go func() {
		for {
			select {
			case <-ticker.C:
				w.OnConsume(ctx, nil)
			}
		}
	}()
}
