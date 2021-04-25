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
	handle             func(ctx context.Context, data []*Message) ([]*Message, error)
	Retry              func(ctx context.Context, message *Message) error
	RetryCountName     string
	Error              func(ctx context.Context, message *Message) error
	Goroutine          bool
	messages           []*Message
	latestExecutedTime time.Time
	mux                sync.Mutex
	LogError           func(context.Context, string)
	LogInfo            func(context.Context, string)
}

func NewBatchWorkerByConfig(batchConfig BatchWorkerConfig, handle func(context.Context, []*Message) ([]*Message, error), retry func(context.Context, *Message) error, retryCountName string, errorHandler func(context.Context, *Message) error, logs ...func(context.Context, string)) *DefaultBatchWorker {
	return NewBatchWorker(batchConfig.BatchSize, batchConfig.Timeout, batchConfig.LimitRetry, handle, retry, retryCountName, errorHandler, batchConfig.Goroutines, logs...)
}

func NewDefaultBatchWorker(batchConfig BatchWorkerConfig, handle func(context.Context, []*Message) ([]*Message, error), retry func(context.Context, *Message) error, logs ...func(context.Context, string)) *DefaultBatchWorker {
	return NewBatchWorker(batchConfig.BatchSize, batchConfig.Timeout, batchConfig.LimitRetry, handle, retry, "", nil, batchConfig.Goroutines, logs...)
}

func NewBatchWorker(batchSize int, timeout int64, limitRetry int, handle func(context.Context, []*Message) ([]*Message, error), retry func(context.Context, *Message) error, retryCountName string, handleError func(context.Context, *Message) error, goroutine bool, logs ...func(context.Context, string)) *DefaultBatchWorker {
	if len(retryCountName) == 0 {
		retryCountName = "retryCount"
	}
	if handleError == nil {
		e1 := NewErrorHandler(logs...)
		handleError = e1.HandleError
	}
	w := &DefaultBatchWorker{
		batchSize:      batchSize,
		timeout:        timeout,
		limitRetry:     limitRetry,
		handle:         handle,
		Retry:          retry,
		RetryCountName: retryCountName,
		Error:          handleError,
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

func (w *DefaultBatchWorker) Handle(ctx context.Context, message *Message) {
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
	batchSize := len(w.messages)
	t := w.latestExecutedTime.Add(time.Duration(w.timeout) * time.Millisecond)
	if batchSize > 0 && (batchSize >= w.batchSize || t.Sub(now) < 0) {
		isReady = true
	}
	if isReady && w.LogInfo != nil {
		w.LogInfo(ctx, fmt.Sprintf("Run: %d / %d - Next %s - Last %s - Timeout: %d", batchSize, w.batchSize, t.Format(TimeFormat), w.latestExecutedTime.Format(TimeFormat), w.timeout))
	}
	return isReady
}

func (w *DefaultBatchWorker) execute(ctx context.Context) {
	lenMessages := len(w.messages)
	if lenMessages == 0 {
		w.reset(ctx)
		return
	}

	errList, err := w.handle(ctx, w.messages)

	if err != nil && w.LogError != nil {
		w.LogError(ctx, "Error of batch handling: "+err.Error())
	}
	if errList != nil && len(errList) > 0 {
		if w.Retry == nil {
			if w.LogError != nil {
				l := len(errList)
				for i := 0; i < l; i++ {
					x := logMessage{Id: errList[i].Id, Data: errList[i].Data, Attributes: errList[i].Attributes}
					w.LogError(ctx, fmt.Sprintf("Error message: %s.", x))
				}
			}
		} else {
			l := len(errList)
			for i := 0; i < l; i++ {
				retryCount := 0
				if errList[i].Attributes == nil {
					errList[i].Attributes = make(map[string]string)
				} else {
					var er4 error
					retryCount, er4 = strconv.Atoi(errList[i].Attributes[w.RetryCountName])
					if er4 != nil {
						retryCount = 1
					}
				}
				retryCount++
				if retryCount > w.limitRetry {
					if w.LogInfo != nil {
						x := logMessage{Id: errList[i].Id, Data: errList[i].Data, Attributes: errList[i].Attributes}
						w.LogInfo(ctx, fmt.Sprintf("Retry: %d . Retry limitation: %d . Message: %s.", retryCount, w.limitRetry, x))
					}
					if w.Error != nil {
						w.Error(ctx, errList[i])
					}
					continue
				} else if w.LogInfo != nil {
					x := logMessage{Id: errList[i].Id, Data: errList[i].Data, Attributes: errList[i].Attributes}
					w.LogInfo(ctx, fmt.Sprintf("Retry: %d . Message: %s.", retryCount, x))
				}
				errList[i].Attributes[w.RetryCountName] = strconv.Itoa(retryCount)
				er3 := w.Retry(ctx, errList[i])
				if er3 != nil && w.LogError != nil {
					x := logMessage{Id: errList[i].Id, Data: errList[i].Data, Attributes: errList[i].Attributes}
					w.LogError(ctx, fmt.Sprintf("Cannot retry %s . Error: %s", x, er3.Error()))
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

func (w *DefaultBatchWorker) Run(ctx context.Context) {
	w.reset(ctx)
	ticker := time.NewTicker(time.Duration(w.timeout) * time.Millisecond)
	go func() {
		for {
			select {
			case <-ticker.C:
				w.Handle(ctx, nil)
			}
		}
	}()
}
