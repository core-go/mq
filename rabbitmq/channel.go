package rabbitmq

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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
