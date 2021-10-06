package mq

import (
	"context"
	"fmt"
	"time"
)

type RetryWriter struct {
	write       func(ctx context.Context, model interface{}) error
	Retries    []time.Duration
	Log        func(context.Context, string)
	Goroutines bool
}

func NewWriterByConfig(write func(context.Context, interface{}) error, goroutines bool, log func(context.Context, string), c *RetryConfig) *RetryWriter {
	if c == nil {
		return &RetryWriter{write: write, Log: log, Goroutines: goroutines}
	} else {
		retries := DurationsFromValue(*c, "Retry", 20)
		if len(retries) == 0 {
			return &RetryWriter{write: write, Log: log, Goroutines: goroutines}
		}
		return &RetryWriter{write: write, Log: log, Retries: retries, Goroutines: goroutines}
	}
}
func NewWriter(write func(context.Context, interface{}) error, goroutines bool, log func(context.Context, string), retries ...time.Duration) *RetryWriter {
	return &RetryWriter{write: write, Log: log, Retries: retries, Goroutines: goroutines}
}
func (c *RetryWriter) Write(ctx context.Context, model interface{}) error {
	if !c.Goroutines {
		return WriteTo(ctx, c.write, model, c.Log, c.Retries...)
	} else {
		go WriteTo(ctx, c.write, model, c.Log, c.Retries...)
		return nil
	}
}
func WriteTo(ctx context.Context, write func(context.Context, interface{}) error, model interface{}, log func(context.Context, string), retries ...time.Duration) error {
	l := len(retries)
	if l == 0 {
		return write(ctx, model)
	} else {
		return WriteWithRetries(ctx, write, model, retries, log)
	}
}

func WriteWithRetries(ctx context.Context, write func(context.Context, interface{}) error, model interface{}, retries []time.Duration, log func(context.Context, string)) error {
	er1 := write(ctx, model)
	if er1 == nil {
		return er1
	}
	i := 0
	err := Retry(ctx, retries, func() (err error) {
		i = i + 1
		er2 := write(ctx, model)

		if er2 == nil && log != nil {
			log(ctx, fmt.Sprintf("Write successfully after %d retries %s", i, model))
		}
		return er2
	}, log)
	if err != nil && log != nil {
		log(ctx, fmt.Sprintf("Failed to write after %d retries: %s. Error: %s.", len(retries), model, err.Error()))
	}
	return err
}
