package kafka

import (
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"time"
)

func GetDialer(username string, password string, algorithm scram.Algorithm, dialer *kafka.Dialer) *kafka.Dialer {
	if dialer == nil {
		dialer = &kafka.Dialer{
			Timeout:   1 * time.Minute,
			DualStack: true,
		}
	}
	if username != "" && password != "" {
		mechanism, err := scram.Mechanism(algorithm, username, password)
		if err != nil {
			panic(err)
		}
		dialer.SASLMechanism = mechanism
	}
	return dialer
}
