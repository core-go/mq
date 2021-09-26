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
}

func NewSimpleSubscriber(conn *nats.Conn, subject string, header bool) *SimpleSubscriber {
	return &SimpleSubscriber{conn, subject, header}
}

func NewSimpleSubscriberByConfig(c SubscriberConfig) (*SimpleSubscriber, error) {
	if c.Connection.Retry.Retry1 <= 0 {
		conn, err := nats.Connect(c.Connection.Url, c.Connection.Options)
		if err != nil {
			return nil, err
		}
		return NewSimpleSubscriber(conn, c.Subject, c.Header), nil
	} else {
		durations := DurationsFromValue(c.Connection.Retry, "Retry", 9)
		conn, err := NewConn(durations, c.Connection.Url, c.Connection.Options)
		if err != nil {
			return nil, err
		}
		return NewSimpleSubscriber(conn, c.Subject, c.Header), nil
	}
}

func (c *SimpleSubscriber) Subscribe(ctx context.Context, handle func(context.Context, []byte, map[string]string, error) error) {
	if c.Header {
		c.Conn.Subscribe(c.Subject, func(msg *nats.Msg) {
			attrs := HeaderToMap(http.Header(msg.Header))
			handle(ctx, msg.Data, attrs, nil)
		})
		c.Conn.Flush()
		runtime.Goexit()
	} else {
		c.Conn.Subscribe(c.Subject, func(msg *nats.Msg) {
			handle(ctx, msg.Data, nil, nil)
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
