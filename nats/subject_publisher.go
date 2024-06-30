package nats

import (
	"context"
	"github.com/nats-io/nats.go"
)

type SubjectPublisher struct {
	Conn *nats.Conn
}

func NewSubjectPublisher(conn *nats.Conn) *SubjectPublisher {
	return &SubjectPublisher{conn}
}
func NewSubjectPublisherByConfig(p PublisherConfig) (*SubjectPublisher, error) {
	if p.Connection.Retry.Retry1 <= 0 {
		conn, err := nats.Connect(p.Connection.Url, p.Connection.Option)
		if err != nil {
			return nil, err
		}
		return NewSubjectPublisher(conn), nil
	} else {
		durations := DurationsFromValue(p.Connection.Retry, "Retry", 9)
		conn, err := NewConn(durations, p.Connection.Url, p.Connection.Option)
		if err != nil {
			return nil, err
		}
		return NewSubjectPublisher(conn), nil
	}
}
func (p *SubjectPublisher) Publish(ctx context.Context, subject string, data []byte, attributes map[string]string) error {
	defer p.Conn.Flush()
	if attributes == nil {
		return p.Conn.Publish(subject, data)
	} else {
		header := MapToHeader(attributes)
		var msg = &nats.Msg{
			Subject: subject,
			Data:    data,
			Reply:   "",
			Header:  nats.Header(*header),
		}
		return p.Conn.PublishMsg(msg)
	}
}
func (p *SubjectPublisher) PublishData(ctx context.Context, subject string, data []byte) error {
	defer p.Conn.Flush()
	return p.Conn.Publish(subject, data)
}