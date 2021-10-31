package amq

import (
	"context"
	"github.com/go-stomp/stomp"
)

type SimpleSender struct {
	Conn        *stomp.Conn
	ContentType string
	Convert     func(context.Context, []byte) ([]byte, error)
}

func NewSimpleSender(client *stomp.Conn, contentType string, options ...func(context.Context, []byte) ([]byte, error)) *SimpleSender {
	if len(contentType) == 0 {
		contentType = "text/plain"
	}
	var convert func(context.Context, []byte) ([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &SimpleSender{Conn: client, ContentType: contentType, Convert: convert}
}

func NewSimpleSenderByConfig(c Config, contentType string, options ...func(context.Context, []byte) ([]byte, error)) (*SimpleSender, error) {
	conn, err := NewConn(c.UserName, c.Password, c.Addr)
	if err != nil {
		return nil, err
	}
	return NewSimpleSender(conn, contentType, options...), nil
}

func (p *SimpleSender) Send(ctx context.Context, destination string, data []byte, attributes map[string]string) (string, error) {
	opts := MapToFrame(attributes)
	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, data)
		if err != nil {
			return "", err
		}
	}
	err = p.Conn.Send(destination, p.ContentType, binary, opts...)
	return "", err
}
