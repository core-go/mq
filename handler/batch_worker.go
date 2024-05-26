package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"
)

const TimeFormat = "15:04:05.000"

type BatchWorker[T any] struct {
	batchSize          int
	timeout            int64
	limitRetry         int
	handle             func(ctx context.Context, data []Message[T]) ([]Message[T], error)
	Validate           func(context.Context, *T) ([]ErrorMessage, error)
	HandleError        func(context.Context, *T, []ErrorMessage, []byte, map[string]string)
	Retry              func(context.Context, []byte, map[string]string) error
	RetryCountName     string
	Error              func(context.Context, []byte, map[string]string) error
	Goroutine          bool
	messages           []Message[T]
	latestExecutedTime time.Time
	mux                sync.Mutex
	Key                string
	LogError           func(context.Context, string)
	LogInfo            func(context.Context, string)
}

func NewBatchWorker[T any](batchSize int, timeout int64, limitRetry int, handle func(context.Context, []Message[T]) ([]Message[T], error), retry func(context.Context, []byte, map[string]string) error, retryCountName string, handleError func(context.Context, []byte, map[string]string) error, goroutine bool, logs ...func(context.Context, string)) *BatchWorker[T] {
	if len(retryCountName) == 0 {
		retryCountName = "retryCount"
	}

	w := &BatchWorker[T]{
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

func (w *BatchWorker[T]) Handle(ctx context.Context, data []byte, attrs map[string]string) {
	if data == nil {
		return
	}
	if w.LogInfo != nil {
		key := GetString(ctx, w.Key)
		if len(key) > 0 {
			w.LogInfo(ctx, fmt.Sprintf("Received message with key %s : %s", key, data))
		} else {
			w.LogInfo(ctx, fmt.Sprintf("Received message: %s", data))
		}
	}
	var v T
	er1 := json.Unmarshal(data, &v)
	if er1 != nil {
		if w.LogError != nil {
			w.LogError(ctx, fmt.Sprintf("cannot unmarshal item: %s. Error: %s", data, er1.Error()))
		}
		return
	}
	if w.Validate != nil {
		errs, err := w.Validate(ctx, &v)
		if err != nil {
			if w.LogError != nil {
				w.LogError(ctx, "Error when validate data: "+err.Error())
			}
			return
		}
		if len(errs) > 0 {
			w.HandleError(ctx, &v, errs, data, attrs)
			return
		}
	}
	w.mux.Lock()
	msg := Message[T]{Data: data, Attributes: attrs, Value: v}
	w.messages = append(w.messages, msg)
	if w.ready(ctx) {
		w.execute(ctx)
	}
	w.mux.Unlock()
}

func (w *BatchWorker[T]) ready(ctx context.Context) bool {
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

func (w *BatchWorker[T]) execute(ctx context.Context) {
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
					x := CreateLog(errList[i].Data, errList[i].Attributes)
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
						x := CreateLog(errList[i].Data, errList[i].Attributes)
						w.LogInfo(ctx, fmt.Sprintf("Retry: %d . Retry limitation: %d . Message: %s.", retryCount, w.limitRetry, x))
					}
					if w.Error != nil {
						w.Error(ctx, errList[i].Data, errList[i].Attributes)
					}
					continue
				} else if w.LogInfo != nil {
					x := CreateLog(errList[i].Data, errList[i].Attributes)
					w.LogInfo(ctx, fmt.Sprintf("Retry: %d . Message: %s.", retryCount, x))
				}
				errList[i].Attributes[w.RetryCountName] = strconv.Itoa(retryCount)
				er3 := w.Retry(ctx, errList[i].Data, errList[i].Attributes)
				if er3 != nil && w.LogError != nil {
					x := CreateLog(errList[i].Data, errList[i].Attributes)
					w.LogError(ctx, fmt.Sprintf("Cannot retry %s . Error: %s", x, er3.Error()))
				}
			}
		}
	}
	w.reset(ctx)
}

func (w *BatchWorker[T]) reset(ctx context.Context) {
	w.messages = w.messages[:0]
	w.latestExecutedTime = time.Now()
}

func (w *BatchWorker[T]) Run(ctx context.Context) {
	w.reset(ctx)
	ticker := time.NewTicker(time.Duration(w.timeout) * time.Millisecond)
	go func() {
		for {
			select {
			case <-ticker.C:
				w.Handle(ctx, nil, nil)
			}
		}
	}()
}
func CreateLog(data []byte, header map[string]string) interface{} {
	if header == nil || len(header) == 0 {
		return data
	}
	m := make(map[string]interface{})
	m["data"] = data
	if header != nil && len(header) > 0 {
		m["attributes"] = header
	}
	return m
}
