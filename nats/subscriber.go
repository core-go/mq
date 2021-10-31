package nats

import (
	"context"
	"github.com/core-go/mq"
	"github.com/nats-io/nats.go"
	"net/http"
	"runtime"
)

type Subscriber struct {
	Conn    *nats.Conn
	Subject string
	Header  bool
	Convert func(context.Context, []byte) ([]byte, error)
}

func NewSubscriber(conn *nats.Conn, subject string, header bool, options...func(context.Context, []byte)([]byte, error)) *Subscriber {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Subscriber{conn, subject, header, convert}
}

func NewSubscriberByConfig(c SubscriberConfig, options...func(context.Context, []byte)([]byte, error)) (*Subscriber, error) {
	if c.Connection.Retry.Retry1 <= 0 {
		conn, err := nats.Connect(c.Connection.Url, c.Connection.Options)
		if err != nil {
			return nil, err
		}
		return NewSubscriber(conn, c.Subject, c.Header, options...), nil
	} else {
		durations := DurationsFromValue(c.Connection.Retry, "Retry", 9)
		conn, err := NewConn(durations, c.Connection.Url, c.Connection.Options)
		if err != nil {
			return nil, err
		}
		return NewSubscriber(conn, c.Subject, c.Header, options...), nil
	}
}

func (c *Subscriber) Subscribe(ctx context.Context, handle func(context.Context, *mq.Message, error) error) {
	if c.Header {
		c.Conn.Subscribe(c.Subject, func(msg *nats.Msg) {
			attrs := HeaderToMap(http.Header(msg.Header))
			message := &mq.Message{
				Data:       msg.Data,
				Attributes: attrs,
				Raw:        msg,
			}
			if c.Convert == nil {
				handle(ctx, message, nil)
			} else {
				data, err := c.Convert(ctx, msg.Data)
				if err == nil {
					message.Data = data
				}
				handle(ctx, message, nil)
			}

		})
		c.Conn.Flush()
		runtime.Goexit()
	} else {
		c.Conn.Subscribe(c.Subject, func(msg *nats.Msg) {
			message := &mq.Message{
				Data: msg.Data,
				Raw:  msg,
			}
			if c.Convert == nil {
				handle(ctx, message, nil)
			} else {
				data, err := c.Convert(ctx, msg.Data)
				if err == nil {
					message.Data = data
				}
				handle(ctx, message, nil)
			}
		})
		c.Conn.Flush()
		runtime.Goexit()
	}
}
