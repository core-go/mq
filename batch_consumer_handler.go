package mq

import (
	"context"
	"fmt"
)

type BatchConsumerHandler struct {
	receive  func(ctx context.Context, message *Message)
	Validate func(ctx context.Context, message *Message) error
	LogError func(context.Context, string)
	LogInfo  func(context.Context, string)
}

func NewBatchConsumerHandler(receive func(context.Context, *Message), validate func(context.Context, *Message) error, logs ...func(context.Context, string)) *BatchConsumerHandler {
	b := BatchConsumerHandler{receive: receive, Validate: validate}
	if len(logs) >= 1 {
		b.LogError = logs[0]
	}
	if len(logs) >= 2 {
		b.LogInfo = logs[1]
	}
	return &b
}

func (c *BatchConsumerHandler) Handle(ctx context.Context, message *Message, err error) error {
	if err != nil {
		if c.LogError != nil {
			c.LogError(ctx, fmt.Sprintf("Processing message error: %s", err.Error()))
		}
		return err
	} else if message == nil {
		return nil
	}
	if c.LogInfo != nil {
		c.LogInfo(ctx, fmt.Sprintf("Received message: %s", message.Data))
	}
	if c.Validate != nil {
		er2 := c.Validate(ctx, message)
		if er2 != nil {
			if c.LogError != nil {
				l := logMessage{Id: message.Id, Data: message.Data, Attributes: message.Attributes}
				c.LogError(ctx, fmt.Sprintf("Message is invalid: %s . Error: %s", l, er2.Error()))
			}
			return er2
		}
	}
	c.receive(ctx, message)
	return nil
}
