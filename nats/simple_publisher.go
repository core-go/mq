package nats

import (
	"context"
	"github.com/nats-io/nats.go"
)

type SimplePublisher struct {
	Conn    *nats.Conn
}

func NewSimplePublisher(conn *nats.Conn) *SimplePublisher {
	return &SimplePublisher{conn}
}
func NewSimplePublisherByConfig(p PublisherConfig) (*SimplePublisher, error) {
	if p.Connection.Retry.Retry1 <= 0 {
		conn, err := nats.Connect(p.Connection.Url, p.Connection.Options)
		if err != nil {
			return nil, err
		}
		return NewSimplePublisher(conn), nil
	} else {
		durations := DurationsFromValue(p.Connection.Retry, "Retry", 9)
		conn, err := NewConn(durations, p.Connection.Url, p.Connection.Options)
		if err != nil {
			return nil, err
		}
		return NewSimplePublisher(conn), nil
	}
}
func (p *SimplePublisher) Publish(ctx context.Context, subject string, data []byte, attributes map[string]string) (string, error) {
	defer p.Conn.Flush()
	if attributes == nil {
		err := p.Conn.Publish(subject, data)
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
