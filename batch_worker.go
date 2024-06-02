package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"
)

const TimeFormat = "15:04:05.000"

type BatchConfig struct {
	RetryCountName string `yaml:"retry_count_name" mapstructure:"retry_count_name" json:"retryCountName,omitempty" gorm:"column:retrycountname" bson:"retryCountName,omitempty" dynamodbav:"retryCountName,omitempty" firestore:"retryCountName,omitempty"`
	LimitRetry     int    `yaml:"limit_retry" mapstructure:"limit_retry" json:"limitRetry,omitempty" gorm:"column:limitretry" bson:"limitRetry,omitempty" dynamodbav:"limitRetry,omitempty" firestore:"limitRetry,omitempty"`
	Goroutines     bool   `yaml:"goroutines" mapstructure:"goroutines" json:"goroutines,omitempty" gorm:"column:goroutines" bson:"goroutines,omitempty" dynamodbav:"goroutines,omitempty" firestore:"goroutines,omitempty"`
	Key            string `yaml:"key" mapstructure:"key" json:"key,omitempty" gorm:"column:key" bson:"key,omitempty" dynamodbav:"key,omitempty" firestore:"key,omitempty"`
	Timeout        int64  `yaml:"timeout" mapstructure:"timeout" json:"timeout,omitempty" gorm:"column:timeout" bson:"timeout,omitempty" dynamodbav:"timeout,omitempty" firestore:"timeout,omitempty"`
	BatchSize      int    `yaml:"batch_size" mapstructure:"batch_size" json:"batchSize,omitempty" gorm:"column:batchsize" bson:"batchSize,omitempty" dynamodbav:"batchSize,omitempty" firestore:"batchSize,omitempty"`
}

type BatchWorker[T any] struct {
	batchSize          int
	timeout            int64
	Unmarshal          func(data []byte, v any) error
	handle             func(ctx context.Context, data []Message[T]) ([]Message[T], error)
	Validate           func(context.Context, *T) ([]ErrorMessage, error)
	Reject             func(context.Context, *T, []ErrorMessage, []byte, map[string]string)
	HandleError        func(context.Context, []byte, map[string]string)
	Retry              func(context.Context, []byte, map[string]string) error
	LimitRetry         int
	RetryCountName     string
	Goroutine          bool
	messages           []Message[T]
	latestExecutedTime time.Time
	mux                sync.Mutex
	Key                string
	LogError           func(context.Context, string)
	LogInfo            func(context.Context, string)
	LogDebug           func(context.Context, string)
}

func NewBatchWorkerByConfig[T any](
	c BatchConfig,
	handle func(context.Context, []Message[T]) ([]Message[T], error),
	validate func(context.Context, *T) ([]ErrorMessage, error),
	reject func(context.Context, *T, []ErrorMessage, []byte, map[string]string),
	handleError func(context.Context, []byte, map[string]string),
	retry func(context.Context, []byte, map[string]string) error,
	logs ...func(context.Context, string)) *BatchWorker[T] {
	return NewBatchWorker[T](c.BatchSize, c.Timeout, nil, handle, validate, reject, handleError, retry, c.LimitRetry, c.RetryCountName, c.Goroutines, c.Key, logs...)
}
func NewBatchWorkerByConfigAndUnmarshal[T any](
	c BatchConfig,
	unmarshal func(data []byte, v any) error,
	handle func(context.Context, []Message[T]) ([]Message[T], error),
	validate func(context.Context, *T) ([]ErrorMessage, error),
	reject func(context.Context, *T, []ErrorMessage, []byte, map[string]string),
	handleError func(context.Context, []byte, map[string]string),
	retry func(context.Context, []byte, map[string]string) error,
	logs ...func(context.Context, string)) *BatchWorker[T] {
	return NewBatchWorker[T](c.BatchSize, c.Timeout, unmarshal, handle, validate, reject, handleError, retry, c.LimitRetry, c.RetryCountName, c.Goroutines, c.Key, logs...)
}
func NewBatchWorker[T any](
	batchSize int, timeout int64,
	unmarshal func(data []byte, v any) error,
	handle func(context.Context, []Message[T]) ([]Message[T], error),
	validate func(context.Context, *T) ([]ErrorMessage, error),
	reject func(context.Context, *T, []ErrorMessage, []byte, map[string]string),
	handleError func(context.Context, []byte, map[string]string),
	retry func(context.Context, []byte, map[string]string) error,
	limitRetry int,
	retryCountName string,
	goroutine bool,
	key string,
	logs ...func(context.Context, string)) *BatchWorker[T] {
	if len(retryCountName) == 0 {
		retryCountName = "retryCount"
	}
	if unmarshal == nil {
		unmarshal = json.Unmarshal
	}
	w := &BatchWorker[T]{
		batchSize:      batchSize,
		timeout:        timeout,
		Unmarshal:      unmarshal,
		handle:         handle,
		Validate:       validate,
		Reject:         reject,
		HandleError:    handleError,
		Retry:          retry,
		LimitRetry:     limitRetry,
		RetryCountName: retryCountName,
		Goroutine:      goroutine,
		Key:            key,
	}
	if len(logs) > 0 {
		w.LogError = logs[0]
	}
	if len(logs) > 1 {
		w.LogInfo = logs[1]
	}
	if len(logs) > 2 {
		w.LogInfo = logs[2]
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
			w.LogInfo(ctx, fmt.Sprintf("Received message with key %s : %s", key, GetLog(data, attrs)))
		} else {
			w.LogInfo(ctx, fmt.Sprintf("Received message: %s", GetLog(data, attrs)))
		}
	}
	var v T
	er1 := json.Unmarshal(data, &v)
	if er1 != nil {
		if w.LogError != nil {
			w.LogError(ctx, fmt.Sprintf("cannot unmarshal item: %s . Error: %s", GetLog(data, attrs), er1.Error()))
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
			w.Reject(ctx, &v, errs, data, attrs)
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
func (w *BatchWorker[T]) CallByTimer(ctx context.Context) {
	w.mux.Lock()
	if w.LogDebug != nil {
		w.LogDebug(ctx, "Call by timer")
	}
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
		if w.LogDebug != nil && batchSize >= w.batchSize {
			w.LogDebug(ctx, fmt.Sprintf("Call by batch size %d %d", w.batchSize, batchSize))
		}
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
	if len(errList) > 0 {
		if w.Retry == nil {
			if w.LogError != nil {
				l := len(errList)
				for i := 0; i < l; i++ {
					w.LogError(ctx, fmt.Sprintf("Error message: %s.", GetLog(errList[i].Data, errList[i].Attributes)))
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
				if retryCount > w.LimitRetry {
					if w.LogInfo != nil {
						w.LogInfo(ctx, fmt.Sprintf("Retry: %d . Retry limitation: %d . Message: %s.", retryCount-1, w.LimitRetry, GetLog(errList[i].Data, errList[i].Attributes)))
					}
					if w.HandleError != nil {
						w.HandleError(ctx, errList[i].Data, errList[i].Attributes)
					}
					continue
				} else if w.LogInfo != nil {
					w.LogInfo(ctx, fmt.Sprintf("Retry: %d . Message: %s", retryCount-1, GetLog(errList[i].Data, errList[i].Attributes)))
				}
				errList[i].Attributes[w.RetryCountName] = strconv.Itoa(retryCount)
				er3 := w.Retry(ctx, errList[i].Data, errList[i].Attributes)
				if er3 != nil && w.LogError != nil {
					w.LogError(ctx, fmt.Sprintf("Cannot retry %s . Error: %s", GetLog(errList[i].Data, errList[i].Attributes), er3.Error()))
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
				w.CallByTimer(ctx)
			}
		}
	}()
}
