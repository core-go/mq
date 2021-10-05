package amq

import (
	"context"
	"github.com/go-stomp/stomp"
)

type SimpleSender struct {
	Conn        *stomp.Conn
	ContentType string
}

func NewSimpleSender(client *stomp.Conn, contentType string) *SimpleSender {
	if len(contentType) == 0 {
		contentType = "text/plain"
	}
	return &SimpleSender{Conn: client, ContentType: contentType}
}

func NewSimpleSenderByConfig(c Config, contentType string) (*SimpleSender, error) {
	conn, err := NewConn(c.UserName, c.Password, c.Addr)
	if err != nil {
		return nil, err
	}
	return NewSimpleSender(conn, contentType), nil
}

func (p *SimpleSender) Send(ctx context.Context, destination string, data []byte, attributes map[string]string) (string, error) {
	opts := MapToFrame(attributes)
	err := p.Conn.Send(destination, p.ContentType, data, opts...)
	return "", err
}
