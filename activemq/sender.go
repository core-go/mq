package activemq

import (
	"context"
	"github.com/go-stomp/stomp"
	"github.com/go-stomp/stomp/frame"
)

type Sender struct {
	Conn        *stomp.Conn
	Destination string
	ContentType string
}

func NewSender(client *stomp.Conn, destinationName string, subscriptionName string, contentType string) *Sender {
	des := destinationName + "::" + subscriptionName
	if len(contentType) == 0 {
		contentType = "text/plain"
	}
	return &Sender{Conn: client, Destination: des, ContentType: contentType}
}

func NewSenderByConfig(c Config, contentType string) (*Sender, error) {
	conn, err := NewConn(c.UserName, c.Password, c.Addr)
	if err != nil {
		return nil, err
	}
	return NewSender(conn, c.DestinationName, c.SubscriptionName, contentType), nil
}
func (p *Sender) SendWithFrame(ctx context.Context, data []byte, attributes map[string]string) error {
	opts := MapToFrame(attributes)
	return p.Conn.Send(p.Destination, p.ContentType, data, opts...)
}
func (p *Sender) Send(ctx context.Context, data []byte) error {
	return p.Conn.Send(p.Destination, p.ContentType, data)
}
func MapToFrame(attributes map[string]string) []func(*frame.Frame) error {
	opts := make([]func(*frame.Frame) error, 0)
	if attributes != nil {
		for k, v := range attributes {
			opts = append(opts, stomp.SendOpt.Header(k, v))
		}
	}
	return opts
}
