package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"net/http"
	"runtime"
)

type Subscriber struct {
	Conn     *nats.Conn
	Subject  string
	LogError func(ctx context.Context, msg string)
}

func NewSubscriber(conn *nats.Conn, subject string, logError func(ctx context.Context, msg string)) *Subscriber {
	return &Subscriber{conn, subject, logError}
}

func NewSubscriberByConfig(c SubscriberConfig, logError func(ctx context.Context, msg string)) (*Subscriber, error) {
	if c.Connection.Retry.Retry1 <= 0 {
		conn, err := nats.Connect(c.Connection.Url, c.Connection.Option)
		if err != nil {
			return nil, err
		}
		return NewSubscriber(conn, c.Subject, logError), nil
	} else {
		durations := DurationsFromValue(c.Connection.Retry, "Retry", 9)
		conn, err := NewConn(durations, c.Connection.Url, c.Connection.Option)
		if err != nil {
			return nil, err
		}
		return NewSubscriber(conn, c.Subject, logError), nil
	}
}
func (c *Subscriber) SubscribeMsg(ctx context.Context, handle func(context.Context, *nats.Msg)) {
	c.Conn.Subscribe(c.Subject, func(msg *nats.Msg) {
		handle(ctx, msg)
	})
	c.Conn.Flush()
	runtime.Goexit()
}
func (c *Subscriber) SubscribeData(ctx context.Context, handle func(context.Context, []byte)) {
	c.Conn.Subscribe(c.Subject, func(msg *nats.Msg) {
		handle(ctx, msg.Data)
	})
	c.Conn.Flush()
	runtime.Goexit()
}
func (c *Subscriber) Subscribe(ctx context.Context, handle func(context.Context, []byte, map[string]string)) {
	c.Conn.Subscribe(c.Subject, func(msg *nats.Msg) {
		attrs := HeaderToMap(http.Header(msg.Header))
		handle(ctx, msg.Data, attrs)
	})
	c.Conn.Flush()
	runtime.Goexit()
}

func HeaderToMap(header http.Header) map[string]string {
	attributes := make(map[string]string)
	for name, values := range header {
		for _, value := range values {
			attributes[name] = value
		}
	}
	return attributes
}
