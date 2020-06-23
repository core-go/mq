package mq

import (
	"context"
	"github.com/sirupsen/logrus"
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
		logrus.Errorf("Processing message error: %s", err.Error())
		return err
	} else if message == nil {
		if logrus.IsLevelEnabled(logrus.WarnLevel) {
			logrus.Warn("Do not proceed empty message")
		}
		return nil
	}

	if c.Validator != nil {
		er2 := c.Validator.Validate(ctx, message)
		if er2 != nil {
			logrus.Errorf("Message is invalid: %v  Error: %s", message, er2.Error())
			return er2
		}
	}
	c.BatchWorker.OnConsume(ctx, message)
	return nil
}
