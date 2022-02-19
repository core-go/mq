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
	Convert     func(context.Context, []byte) ([]byte, error)
}

func NewSender(client *stomp.Conn, destinationName string, subscriptionName string, contentType string, options...func(context.Context, []byte)([]byte, error)) *Sender {
	des := destinationName + "::" + subscriptionName
	if len(contentType) == 0 {
		contentType = "text/plain"
	}
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &Sender{Conn: client, Destination: des, ContentType: contentType, Convert: convert}
}

func NewSenderByConfig(c Config, contentType string) (*Sender, error) {
	conn, err := NewConn(c.UserName, c.Password, c.Addr)
	if err != nil {
		return nil, err
	}
	return NewSender(conn, c.DestinationName, c.SubscriptionName, contentType), nil
}

func (p *Sender) Put(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Send(ctx, data, attributes)
}
func (p *Sender) Write(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Send(ctx, data, attributes)
}
func (p *Sender) Produce(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Send(ctx, data, attributes)
}
func (p *Sender) Publish(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	return p.Send(ctx, data, attributes)
}
func (p *Sender) Send(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	opts := MapToFrame(attributes)
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, data)
		if err != nil {
			return "", err
		}
	}
	err = p.Conn.Send(p.Destination, p.ContentType, binary, opts...)
	return "", err
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
