package mq

import (
	"context"
	"fmt"
)

type Subscription struct {
	receive  func(ctx context.Context, data []byte, header map[string]string)
	Validate func(ctx context.Context, message *Message) error
	LogError func(context.Context, string)
	LogInfo  func(context.Context, string)
}

func NewSubscription(receive func(context.Context, []byte, map[string]string), validate func(context.Context, *Message) error, logs ...func(context.Context, string)) *Subscription {
	b := Subscription{receive: receive, Validate: validate}
	if len(logs) >= 1 {
		b.LogError = logs[0]
	}
	if len(logs) >= 2 {
		b.LogInfo = logs[1]
	}
	return &b
}

func (c *Subscription) Receive(ctx context.Context, data []byte, header map[string]string, err error) error {
	if err != nil {
		if c.LogError != nil {
			c.LogError(ctx, fmt.Sprintf("Processing message error: %s", err.Error()))
		}
		return err
	} else if data == nil {
		return nil
	}
	if c.LogInfo != nil {
		c.LogInfo(ctx, fmt.Sprintf("Received message: %s", data))
	}
	if c.Validate != nil {
		message := &Message{Data: data, Attributes: header}
		er2 := c.Validate(ctx, message)
		if er2 != nil {
			if c.LogError != nil {
				if header == nil || len(header) == 0 {
					c.LogError(ctx, fmt.Sprintf("Message is invalid: %s . Error: %s", data, er2.Error()))
				} else {
					c.LogError(ctx, fmt.Sprintf("Message is invalid: %s %s. Error: %s", data, header, er2.Error()))
				}
			}
			return er2
		}
	}
	c.receive(ctx, data, header)
	return nil
}
