package nats

import (
	"context"
	"github.com/nats-io/nats.go"
)

type SimplePublisher struct {
	Conn    *nats.Conn
	Convert func(context.Context, []byte)([]byte, error)
}

func NewSimplePublisher(conn *nats.Conn, options...func(context.Context, []byte)([]byte, error)) *SimplePublisher {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &SimplePublisher{conn, convert}
}
func NewSimplePublisherByConfig(p PublisherConfig, options...func(context.Context, []byte)([]byte, error)) (*SimplePublisher, error) {
	if p.Connection.Retry.Retry1 <= 0 {
		conn, err := nats.Connect(p.Connection.Url, p.Connection.Options)
		if err != nil {
			return nil, err
		}
		return NewSimplePublisher(conn, options...), nil
	} else {
		durations := DurationsFromValue(p.Connection.Retry, "Retry", 9)
		conn, err := NewConn(durations, p.Connection.Url, p.Connection.Options)
		if err != nil {
			return nil, err
		}
		return NewSimplePublisher(conn, options...), nil
	}
}
func (p *SimplePublisher) Put(ctx context.Context, subject string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, subject, data, attributes)
}
func (p *SimplePublisher) Send(ctx context.Context, subject string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, subject, data, attributes)
}
func (p *SimplePublisher) Write(ctx context.Context, subject string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, subject, data, attributes)
}
func (p *SimplePublisher) Produce(ctx context.Context, subject string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, subject, data, attributes)
}
func (p *SimplePublisher) Publish(ctx context.Context, subject string, data []byte, attributes map[string]string) (string, error) {
	defer p.Conn.Flush()
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, binary)
		if err != nil {
			return "", err
		}
	}
	if attributes == nil {
		err := p.Conn.Publish(subject, binary)
		return "", err
	} else {
		header := MapToHeader(attributes)
		var msg = &nats.Msg{
			Subject: subject,
			Data:    data,
			Reply:   "",
			Header: nats.Header(*header),
		}
		err := p.Conn.PublishMsg(msg)
		return "", err
	}
}
