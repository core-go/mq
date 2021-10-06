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
	Goroutines bool
}

func NewSenderByConfig(send func(context.Context, []byte, map[string]string) (string, error), goroutines bool, log func(context.Context, string), c *RetryConfig) *RetrySender {
	if c == nil {
		return &RetrySender{send: send, Log: log, Goroutines: goroutines}
	} else {
		retries := DurationsFromValue(*c, "Retry", 20)
		if len(retries) == 0 {
			return &RetrySender{send: send, Log: log, Goroutines: goroutines}
		}
		return &RetrySender{send: send, Log: log, Retries: retries, Goroutines: goroutines}
	}
}
func NewSender(send func(context.Context, []byte, map[string]string) (string, error), goroutines bool, log func(context.Context, string), retries ...time.Duration) *RetrySender {
	return &RetrySender{send: send, Log: log, Retries: retries, Goroutines: goroutines}
}
func (c *RetrySender) Send(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	if !c.Goroutines {
		return Send(ctx, c.send, data, attributes, c.Log, c.Retries...)
	} else {
		go Send(ctx, c.send, data, attributes, c.Log, c.Retries...)
		return "", nil
	}
}
func Send(ctx context.Context, send func(context.Context, []byte, map[string]string) (string, error), data []byte, attributes map[string]string, log func(context.Context, string), retries ...time.Duration) (string, error) {
	l := len(retries)
	if l == 0 {
		return send(ctx, data, attributes)
	} else {
		return SendWithRetries(ctx, send, data, attributes, retries, log)
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
