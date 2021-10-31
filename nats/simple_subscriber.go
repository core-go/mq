package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"net/http"
	"runtime"
)

type SimpleSubscriber struct {
	Conn    *nats.Conn
	Subject string
	Header  bool
	Convert func(context.Context, []byte) ([]byte, error)
}

func NewSimpleSubscriber(conn *nats.Conn, subject string, header bool, options...func(context.Context, []byte)([]byte, error)) *SimpleSubscriber {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &SimpleSubscriber{conn, subject, header, convert}
}

func NewSimpleSubscriberByConfig(c SubscriberConfig, options...func(context.Context, []byte)([]byte, error)) (*SimpleSubscriber, error) {
	if c.Connection.Retry.Retry1 <= 0 {
		conn, err := nats.Connect(c.Connection.Url, c.Connection.Options)
		if err != nil {
			return nil, err
		}
		return NewSimpleSubscriber(conn, c.Subject, c.Header, options...), nil
	} else {
		durations := DurationsFromValue(c.Connection.Retry, "Retry", 9)
		conn, err := NewConn(durations, c.Connection.Url, c.Connection.Options)
		if err != nil {
			return nil, err
		}
		return NewSimpleSubscriber(conn, c.Subject, c.Header, options...), nil
	}
}

func (c *SimpleSubscriber) Subscribe(ctx context.Context, handle func(context.Context, []byte, map[string]string, error) error) {
	if c.Header {
		c.Conn.Subscribe(c.Subject, func(msg *nats.Msg) {
			attrs := HeaderToMap(http.Header(msg.Header))
			if c.Convert == nil {
				handle(ctx, msg.Data, attrs, nil)
			} else {
				data, err := c.Convert(ctx, msg.Data)
				handle(ctx, data, attrs, err)
			}
		})
		c.Conn.Flush()
		runtime.Goexit()
	} else {
		c.Conn.Subscribe(c.Subject, func(msg *nats.Msg) {
			if c.Convert == nil {
				handle(ctx, msg.Data, nil, nil)
			} else {
				data, err := c.Convert(ctx, msg.Data)
				handle(ctx, data, nil, err)
			}
		})
		c.Conn.Flush()
		runtime.Goexit()
	}
}

func HeaderToMap(header http.Header) map[string]string {
	attributes := make(map[string]string, 0)
	for name, values := range header {
		for _, value := range values {
			attributes[name] = value
		}
	}
	return attributes
}
