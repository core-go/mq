package rabbitmq

import "github.com/streadway/amqp"

func NewChannel(url string) (*amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	return conn.Channel()
}
