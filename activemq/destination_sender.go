package activemq

import (
	"context"

	"github.com/go-stomp/stomp/v3"
)

type DestinationSender struct {
	Conn        *stomp.Conn
	ContentType string
}

func NewDestinationSender(client *stomp.Conn, contentType string) *DestinationSender {
	if len(contentType) == 0 {
		contentType = "text/plain"
	}
	return &DestinationSender{Conn: client, ContentType: contentType}
}

func NewDestinationSenderByConfig(c Config, contentType string) (*DestinationSender, error) {
	conn, err := NewConn(c.UserName, c.Password, c.Addr)
	if err != nil {
		return nil, err
	}
	return NewDestinationSender(conn, contentType), nil
}
func (p *DestinationSender) SendWithFrame(ctx context.Context, destination string, data []byte, attributes map[string]string) error {
	opts := MapToFrame(attributes)
	return p.Conn.Send(destination, p.ContentType, data, opts...)
}
func (p *DestinationSender) Send(ctx context.Context, destination string, data []byte) error {
	return p.Conn.Send(destination, p.ContentType, data)
}
