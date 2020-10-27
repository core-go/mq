package mq

import (
	"context"
)

type BatchConsumerCaller struct {
	BatchWorker BatchWorker
	Validator   Validator
}

func NewBatchConsumerCaller(batchWorker BatchWorker, validator Validator) *BatchConsumerCaller {
	b := BatchConsumerCaller{batchWorker, validator}
	return &b
}

func (c *BatchConsumerCaller) Call(ctx context.Context, message *Message, err error) error {
	if err != nil {
		Errorf(ctx, "Processing message error: %s", err.Error())
		return err
	} else if message == nil {
		return nil
	}
	if IsDebugEnabled() {
		Debugf(ctx, "Received message: %s", message.Data)
	}
	if c.Validator != nil {
		er2 := c.Validator.Validate(ctx, message)
		if er2 != nil {
			Errorf(ctx, "Message is invalid: %v  Error: %s", message, er2.Error())
			return er2
		}
	}
	c.BatchWorker.OnConsume(ctx, message)
	return nil
}
