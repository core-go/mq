package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

type HandlerConfig struct {
	RetryCountName string `mapstructure:"retry_count_name" json:"retryCountName,omitempty" gorm:"column:retrycountname" bson:"retryCountName,omitempty" dynamodbav:"retryCountName,omitempty" firestore:"retryCountName,omitempty"`
	LimitRetry     int    `mapstructure:"limit_retry" json:"limitRetry,omitempty" gorm:"column:limitretry" bson:"limitRetry,omitempty" dynamodbav:"limitRetry,omitempty" firestore:"limitRetry,omitempty"`
	Goroutines     bool   `mapstructure:"goroutines" json:"goroutines,omitempty" gorm:"column:goroutines" bson:"goroutines,omitempty" dynamodbav:"goroutines,omitempty" firestore:"goroutines,omitempty"`
}
type Handler struct {
	Write          func(ctx context.Context, model interface{}) error
	ModelType      *reflect.Type
	Validate       func(ctx context.Context, message *Message) error
	LimitRetry     int
	Retry          func(context.Context, []byte, map[string]string) error
	RetryCountName string
	Error          func(context.Context, []byte, map[string]string) error
	Retries        []time.Duration
	Goroutines     bool
	LogError       func(context.Context, string)
	LogInfo        func(context.Context, string)
}

func NewHandlerByConfig(c HandlerConfig, write func(context.Context, interface{}) error, modelType *reflect.Type, retry func(context.Context, []byte, map[string]string) error, validate func(context.Context, *Message) error, handleError func(context.Context, []byte, map[string]string) error, logs ...func(context.Context, string)) *Handler {
	return NewHandlerWithRetryService(write, modelType, c.LimitRetry, retry, c.RetryCountName, validate, handleError, c.Goroutines, logs...)
}
func NewHandlerWithRetryConfig(write func(context.Context, interface{}) error, modelType *reflect.Type, validate func(context.Context, *Message) error, c *RetryConfig, goroutines bool, handleError func(context.Context, []byte, map[string]string) error, logs ...func(context.Context, string)) *Handler {
	if c == nil {
		return NewHandlerWithRetries(write, modelType, validate, nil, handleError, goroutines, logs...)
	}
	retries := DurationsFromValue(*c, "Retry", 20)
	if len(retries) == 0 {
		return NewHandlerWithRetries(write, modelType, validate, nil, handleError, goroutines, logs...)
	}
	return NewHandlerWithRetries(write, modelType, validate, retries, handleError, goroutines, logs...)
}
func NewHandlerWithRetries(write func(context.Context, interface{}) error, modelType *reflect.Type, validate func(context.Context, *Message) error, retries []time.Duration, handleError func(context.Context, []byte, map[string]string) error, goroutines bool, logs ...func(context.Context, string)) *Handler {
	c := &Handler{
		ModelType:  modelType,
		Write:      write,
		Validate:   validate,
		Goroutines: goroutines,
		Error:      handleError,
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
func NewHandler(write func(context.Context, interface{}) error, modelType *reflect.Type, validate func(context.Context, *Message) error, goroutines bool, logs ...func(context.Context, string)) *Handler {
	return NewHandlerWithRetryService(write, modelType, -1, nil, "", validate, nil, goroutines, logs...)
}
func NewHandlerWithRetryService(write func(context.Context, interface{}) error, modelType *reflect.Type, limitRetry int, retry func(context.Context, []byte, map[string]string) error, retryCountName string, validate func(context.Context, *Message) error,
	handleError func(context.Context, []byte, map[string]string) error,
	goroutines bool, logs ...func(context.Context, string)) *Handler {
	if len(retryCountName) == 0 {
		retryCountName = "retryCount"
	}
	if retry != nil && handleError == nil {
		e1 := NewErrorHandler(logs...)
		handleError = e1.HandleError
	}
	c := &Handler{
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

func (c *Handler) Handle(ctx context.Context, data []byte, header map[string]string, err error) error {
	if err != nil {
		if c.LogError != nil {
			c.LogError(ctx, "Processing message error: "+err.Error())
		}
		return err
	} else if data == nil {
		return nil
	}
	if c.LogInfo != nil {
		c.LogInfo(ctx, fmt.Sprintf("Received message: %s", data))
	}
	message := &Message{Data: data, Attributes: header}
	if c.Validate != nil {
		er2 := c.Validate(ctx, message)
		if er2 != nil {
			if c.LogError != nil {
				if header == nil || len(header) == 0 {
					c.LogError(ctx, fmt.Sprintf("Message is invalid: %s . Error: %s", data, er2.Error()))
				} else {
					c.LogError(ctx, fmt.Sprintf("Message is invalid: %s %s . Error: %s", data, header, er2.Error()))
				}
			}
			return er2
		}
	}
	var item interface{}
	if message.Value != nil {
		item = message.Value
	} else {
		item = message.Data
	}
	if c.ModelType != nil && item == nil {
		v := InitModel(*c.ModelType)
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
		go c.write(ctx, data, header, item)
		return nil
	} else {
		return c.write(ctx, data, header, item)
	}
}
func (c *Handler) write(ctx context.Context, data []byte, header map[string]string, item interface{}) error {
	// ctx = context.WithValue(ctx, "message", message)
	if c.Retry == nil && c.Retries != nil && len(c.Retries) > 0 {
		return WriteWithRetry(ctx, c.Write, data, item, c.Retries, c.Error, c.LogError)
	} else {
		return Write(ctx, c.Write, data, header, item, c.Error, c.Retry, c.LimitRetry, c.RetryCountName, c.LogError, c.LogInfo)
	}
}

func WriteWithRetry(ctx context.Context, write func(context.Context, interface{}) error, data []byte, item interface{}, retries []time.Duration, handleError func(context.Context, []byte, map[string]string) error, logs ...func(context.Context, string)) error {
	var logError func(context.Context, string)
	if len(logs) > 0 {
		logError = logs[0]
	}
	if er1 := write(ctx, item); er1 != nil {
		i := 0
		err := Retry(ctx, retries, func() (err error) {
			i = i + 1
			er2 := write(ctx, item)
			if er2 == nil && logError != nil {
				logError(ctx, fmt.Sprintf("Write successfully after %d retries %s", i, data))
			}
			return er2
		}, logError)
		if err != nil {
			if handleError != nil {
				handleError(ctx, data, nil)
			}
			if logError != nil {
				logError(ctx, fmt.Sprintf("Failed to write after %d retries: %s. Error: %s.", len(retries), data, er1.Error()))
			}
		}
		return err
	}
	return nil
}
func Write(ctx context.Context, write func(context.Context, interface{}) error, data []byte, attrs map[string]string, item interface{}, handleError func(context.Context, []byte, map[string]string) error, retry func(context.Context, []byte, map[string]string) error, limitRetry int, retryCountName string, logs ...func(context.Context, string)) error {
	var logError func(context.Context, string)
	var logInfo func(context.Context, string)
	if len(logs) > 0 {
		logError = logs[0]
	}
	if len(logs) > 1 {
		logInfo = logs[1]
	}
	er3 := write(ctx, item)
	if er3 == nil {
		return er3
	}
	if logError != nil {
		logError(ctx, fmt.Sprintf("Fail to write %s . Error: %s", data, er3.Error()))
	}

	if retry == nil {
		if handleError != nil {
			handleError(ctx, data, attrs)
		}
		return er3
	}
	retryCount := 0
	if attrs == nil {
		attrs = make(map[string]string)
	} else {
		var er4 error
		retryCount, er4 = strconv.Atoi(attrs[retryCountName])
		if er4 != nil {
			retryCount = 0
		}
	}
	retryCount++
	if retryCount > limitRetry {
		if logInfo != nil {
			if attrs == nil || len(attrs) == 0 {
				logInfo(ctx, fmt.Sprintf("Retry: %d . Retry limitation: %d . Message: %s.", retryCount, limitRetry, data))
			} else {
				logInfo(ctx, fmt.Sprintf("Retry: %d . Retry limitation: %d . Message: %s %s.", retryCount, limitRetry, data, attrs))
			}

		}
		if handleError != nil {
			handleError(ctx, data, attrs)
		}
	} else {
		if logInfo != nil {
			if attrs == nil || len(attrs) == 0 {
				logInfo(ctx, fmt.Sprintf("Retry: %d . Message: %s.", retryCount, data))
			} else {
				logInfo(ctx, fmt.Sprintf("Retry: %d . Message: %s %s.", retryCount, data, attrs))
			}
		}
		attrs[retryCountName] = strconv.Itoa(retryCount)
		er2 := retry(ctx, data, attrs)
		if er2 != nil {
			if logError != nil {
				if attrs == nil || len(attrs) == 0 {
					logError(ctx, fmt.Sprintf("Cannot retry %s . Error: %s", data, er2.Error()))
				} else {
					logError(ctx, fmt.Sprintf("Cannot retry %s %s. Error: %s", data, attrs, er2.Error()))
				}
			}
		}
	}
	return nil
}
