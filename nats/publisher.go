package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"net/http"
)

type Publisher struct {
	Conn    *nats.Conn
	Subject string
	Convert func(context.Context, []byte)([]byte, error)
}

func NewPublisher(conn *nats.Conn, subject string, options...func(context.Context, []byte)([]byte, error)) *Publisher {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Publisher{conn, subject, convert}
}
func NewPublisherByConfig(p PublisherConfig, options...func(context.Context, []byte)([]byte, error)) (*Publisher, error) {
	if p.Connection.Retry.Retry1 <= 0 {
		conn, err := nats.Connect(p.Connection.Url, p.Connection.Options)
		if err != nil {
			return nil, err
		}
		return NewPublisher(conn, p.Subject, options...), nil
	} else {
		durations := DurationsFromValue(p.Connection.Retry, "Retry", 9)
		conn, err := NewConn(durations, p.Connection.Url, p.Connection.Options)
		if err != nil {
			return nil, err
		}
		return NewPublisher(conn, p.Subject, options...), nil
	}
}
func (p *Publisher) Put(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, data, attributes)
}
func (p *Publisher) Send(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, data, attributes)
}
func (p *Publisher) Produce(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, data, attributes)
}
func (p *Publisher) Write(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, data, attributes)
}
func (p *Publisher) Publish(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	defer p.Conn.Flush()
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, data)
		if err != nil {
			return "", err
		}
	}
	if attributes == nil {
		err := p.Conn.Publish(p.Subject, binary)
		return "", err
	} else {
		header := MapToHeader(attributes)
		var msg = &nats.Msg{
			Subject: p.Subject,
			Data:    binary,
			Reply:   "",
			Header: nats.Header(*header),
		}
		err := p.Conn.PublishMsg(msg)
		return "", err
	}
}

func MapToHeader(attributes map[string]string) *http.Header {
	if attributes == nil || len(attributes) == 0 {
		return nil
	}
	header := &http.Header{}
	for k, v := range attributes {
		header.Add(k, v)
	}
	return header
}
