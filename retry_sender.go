package mq

import (
	"context"
	"fmt"
	"time"
)

type RetrySender struct {
	send       func(ctx context.Context, data []byte, attributes map[string]string) (string, error)
	Retries    []time.Duration
	Log        func(context.Context, string)
	Error      func(ctx context.Context, data []byte, attrs map[string]string) error
	Goroutines bool
}

func NewSenderByConfig(send func(context.Context, []byte, map[string]string) (string, error), goroutines bool, log func(context.Context, string), c *RetryConfig, options... func(context.Context, []byte, map[string]string) error) *RetrySender {
	var handlerError func(context.Context, []byte, map[string]string) error
	if len(options) > 0 {
		handlerError = options[0]
	}
	if c == nil {
		return &RetrySender{send: send, Log: log, Goroutines: goroutines, Error: handlerError}
	} else {
		retries := DurationsFromValue(*c, "Retry", 20)
		if len(retries) == 0 {
			return &RetrySender{send: send, Log: log, Goroutines: goroutines}
		}
		return &RetrySender{send: send, Log: log, Retries: retries, Goroutines: goroutines, Error: handlerError}
	}
}
func NewSender(send func(context.Context, []byte, map[string]string) (string, error), goroutines bool, log func(context.Context, string), retries []time.Duration, options... func(context.Context, []byte, map[string]string) error) *RetrySender {
	var handlerError func(context.Context, []byte, map[string]string) error
	if len(options) > 0 {
		handlerError = options[0]
	}
	return &RetrySender{send: send, Log: log, Retries: retries, Goroutines: goroutines, Error: handlerError}
}
func (c *RetrySender) Send(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	if !c.Goroutines {
		return Send(ctx, c.send, data, attributes, c.Log, c.Error, c.Retries...)
	} else {
		go Send(ctx, c.send, data, attributes, c.Log, c.Error, c.Retries...)
		return "", nil
	}
}
func Send(ctx context.Context, send func(context.Context, []byte, map[string]string) (string, error), data []byte, attributes map[string]string, log func(context.Context, string), handlerError func(context.Context, []byte, map[string]string) error, retries ...time.Duration) (string, error) {
	l := len(retries)
	if l == 0 {
		r, err := send(ctx, data, attributes)
		if err != nil && handlerError != nil {
			handlerError(ctx, data, attributes)
		}
		return r, err
	} else {
		r, err := SendWithRetries(ctx, send, data, attributes, retries, log)
		if err != nil && handlerError != nil {
			handlerError(ctx, data, attributes)
		}
		return r, err
	}
}

func SendWithRetries(ctx context.Context, produce func(context.Context, []byte, map[string]string) (string, error), data []byte, attributes map[string]string, retries []time.Duration, log func(context.Context, string)) (string, error) {
	id, er1 := produce(ctx, data, attributes)
	if er1 == nil {
		return id, er1
	}
	i := 0
	err := Retry(ctx, retries, func() (err error) {
		i = i + 1
		id2, er2 := produce(ctx, data, attributes)
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
