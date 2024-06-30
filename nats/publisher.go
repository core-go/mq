package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"net/http"
)

type Publisher struct {
	Conn    *nats.Conn
	Subject string
}

func NewPublisher(conn *nats.Conn, subject string) *Publisher {
	return &Publisher{conn, subject}
}
func NewPublisherByConfig(p PublisherConfig) (*Publisher, error) {
	if p.Connection.Retry.Retry1 <= 0 {
		conn, err := nats.Connect(p.Connection.Url, p.Connection.Option)
		if err != nil {
			return nil, err
		}
		return NewPublisher(conn, p.Subject), nil
	} else {
		durations := DurationsFromValue(p.Connection.Retry, "Retry", 9)
		conn, err := NewConn(durations, p.Connection.Url, p.Connection.Option)
		if err != nil {
			return nil, err
		}
		return NewPublisher(conn, p.Subject), nil
	}
}
func (p *Publisher) Publish(ctx context.Context, data []byte, attributes map[string]string) error {
	defer p.Conn.Flush()
	if attributes == nil {
		return p.Conn.Publish(p.Subject, data)
	} else {
		header := MapToHeader(attributes)
		var msg = &nats.Msg{
			Subject: p.Subject,
			Data:    data,
			Reply:   "",
			Header:  nats.Header(*header),
		}
		return p.Conn.PublishMsg(msg)
	}
}
func (p *Publisher) PublishData(ctx context.Context, data []byte) error {
	defer p.Conn.Flush()
	return p.Conn.Publish(p.Subject, data)
}
func (p *Publisher) PublishMsg(msg *nats.Msg) error {
	if msg == nil {
		return nil
	}
	defer p.Conn.Flush()
	return p.Conn.PublishMsg(msg)
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
