package mq

import (
	"context"
	"fmt"
	"time"
)

type SimpleRetrySender struct {
	send       func(ctx context.Context, topic string, data []byte, attributes map[string]string) (string, error)
	Retries    []time.Duration
	Log        func(context.Context, string)
	Error      func(ctx context.Context, data []byte, attrs map[string]string) error
	Goroutines bool
}

func NewSimpleSenderByConfig(send func(context.Context, string, []byte, map[string]string) (string, error), goroutines bool, log func(context.Context, string), c *RetryConfig, options... func(context.Context, []byte, map[string]string) error) *SimpleRetrySender {
	var handlerError func(context.Context, []byte, map[string]string) error
	if len(options) > 0 {
		handlerError = options[0]
	}
	if c == nil {
		return &SimpleRetrySender{send: send, Log: log, Goroutines: goroutines, Error: handlerError}
	} else {
		retries := DurationsFromValue(*c, "Retry", 20)
		if len(retries) == 0 {
			return &SimpleRetrySender{send: send, Log: log, Goroutines: goroutines}
		}
		return &SimpleRetrySender{send: send, Log: log, Retries: retries, Goroutines: goroutines, Error: handlerError}
	}
}
func NewSimpleSender(send func(context.Context, string, []byte, map[string]string) (string, error), goroutines bool, log func(context.Context, string), retries []time.Duration, options... func(context.Context, []byte, map[string]string) error) *SimpleRetrySender {
	var handlerError func(context.Context, []byte, map[string]string) error
	if len(options) > 0 {
		handlerError = options[0]
	}
	return &SimpleRetrySender{send: send, Log: log, Retries: retries, Goroutines: goroutines, Error: handlerError}
}
func (c *SimpleRetrySender) Send(ctx context.Context, topic string, data []byte, attributes map[string]string) (string, error) {
	if !c.Goroutines {
		return SendTo(ctx, c.send, topic, data, attributes, c.Log, c.Error, c.Retries...)
	} else {
		go SendTo(ctx, c.send, topic, data, attributes, c.Log, c.Error, c.Retries...)
		return "", nil
	}
}
func SendTo(ctx context.Context, send func(context.Context, string, []byte, map[string]string) (string, error), to string, data []byte, attributes map[string]string, log func(context.Context, string), handlerError func(context.Context, []byte, map[string]string) error, retries ...time.Duration) (string, error) {
	l := len(retries)
	if l == 0 {
		r, err := send(ctx, to, data, attributes)
		if err != nil && handlerError != nil {
			handlerError(ctx, data, attributes)
		}
		return r, err
	} else {
		r, err := SendToWithRetries(ctx, send, to, data, attributes, retries, log)
		if err != nil && handlerError != nil {
			handlerError(ctx, data, attributes)
		}
		return r, err
	}
}

func SendToWithRetries(ctx context.Context, send func(context.Context, string, []byte, map[string]string) (string, error), to string, data []byte, attributes map[string]string, retries []time.Duration, log func(context.Context, string)) (string, error) {
	id, er1 := send(ctx, to, data, attributes)
	if er1 == nil {
		return id, er1
	}
	i := 0
	err := Retry(ctx, retries, func() (err error) {
		i = i + 1
		id2, er2 := send(ctx, to, data, attributes)
		id = id2
		if er2 == nil && log != nil {
			log(ctx, fmt.Sprintf("Send successfully after %d retries %s", i, data))
		}
		return er2
	}, log)
	if err != nil && log != nil {
		log(ctx, fmt.Sprintf("Failed to send after %d retries: %s. Error: %s.", len(retries), data, err.Error()))
	}
	return id, err
}
