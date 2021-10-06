package mq

import (
	"context"
	"fmt"
	"time"
)

type RetryWriter struct {
	write      func(ctx context.Context, model interface{}) error
	Retries    []time.Duration
	Log        func(context.Context, string)
	WriteError func(ctx context.Context, model interface{}) error
	Goroutines bool
}

func NewWriterByConfig(write func(context.Context, interface{}) error, goroutines bool, log func(context.Context, string), c *RetryConfig, options ...func(context.Context, interface{}) error) *RetryWriter {
	var writeError func(context.Context, interface{}) error
	if len(options) > 0 {
		writeError = options[0]
	}
	if c == nil {
		return &RetryWriter{write: write, Log: log, Goroutines: goroutines}
	} else {
		retries := DurationsFromValue(*c, "Retry", 20)
		if len(retries) == 0 {
			return &RetryWriter{write: write, Log: log, Goroutines: goroutines, WriteError: writeError}
		}
		return &RetryWriter{write: write, Log: log, Retries: retries, Goroutines: goroutines, WriteError: writeError}
	}
}
func NewWriter(write func(context.Context, interface{}) error, goroutines bool, log func(context.Context, string), retries []time.Duration, options ...func(context.Context, interface{}) error) *RetryWriter {
	var writeError func(context.Context, interface{}) error
	if len(options) > 0 {
		writeError = options[0]
	}
	return &RetryWriter{write: write, Log: log, Retries: retries, Goroutines: goroutines, WriteError: writeError}
}
func (c *RetryWriter) Write(ctx context.Context, model interface{}) error {
	if !c.Goroutines {
		return WriteTo(ctx, c.write, model, c.Log, c.WriteError, c.Retries...)
	} else {
		go WriteTo(ctx, c.write, model, c.Log, c.WriteError, c.Retries...)
		return nil
	}
}
func WriteTo(ctx context.Context, write func(context.Context, interface{}) error, model interface{}, log func(context.Context, string), writeError func(context.Context, interface{}) error, retries ...time.Duration) error {
	l := len(retries)
	if l == 0 {
		err := write(ctx, model)
		if err != nil && writeError != nil {
			writeError(ctx, model)
		}
		return err
	} else {
		err := WriteWithRetries(ctx, write, model, retries, log)
		if err != nil && writeError != nil {
			writeError(ctx, model)
		}
		return err
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
