package rabbitmq

import (
	"github.com/streadway/amqp"
	"time"
)

func NewChannel(url string) (*amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return conn.Channel()
}

func NewChannelWithTimeOut(url string, timeout time.Duration) (*amqp.Channel, error) {
	config := amqp.Config{
		Locale: "en_US",
		Dial:   amqp.DefaultDial(timeout),
	}
	conn, err := amqp.DialConfig(url, config)
	if err != nil {
		return nil, err
	}
	return conn.Channel()
}

