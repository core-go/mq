package mq

import (
	"context"
	"fmt"
)

type BatchConsumerCaller struct {
	BatchWorker BatchWorker
	Validator   Validator
	LogError    func(context.Context, string)
	LogInfo     func(context.Context, string)
}

func NewBatchConsumerCaller(batchWorker BatchWorker, validator Validator, logs ...func(context.Context, string)) *BatchConsumerCaller {
	b := BatchConsumerCaller{BatchWorker: batchWorker, Validator: validator}
	if len(logs) >= 1 {
		b.LogError = logs[0]
	}
	if len(logs) >= 2 {
		b.LogInfo = logs[1]
	}
	return &b
}

func (c *BatchConsumerCaller) Call(ctx context.Context, message *Message, err error) error {
	if err != nil {
		if c.LogError != nil {
			c.LogError(ctx, fmt.Sprintf("Processing message error: %s", err.Error()))
		}
		return err
	} else if message == nil {
		return nil
	}
	if c.LogInfo != nil {
		c.LogInfo(ctx, "Received message: "+string(message.Data))
	}
	if c.Validator != nil {
		er2 := c.Validator.Validate(ctx, message)
		if er2 != nil {
			if c.LogError != nil {
				c.LogError(ctx, fmt.Sprintf("Message is invalid: %s  Error: %s", message, er2.Error()))
			}
			return er2
		}
	}
	c.BatchWorker.OnConsume(ctx, message)
	return nil
}
