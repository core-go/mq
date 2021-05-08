package kafka

import (
	"context"
	"net"
	"time"
)

type HealthChecker struct {
	Brokers []string
	Service string
	Timeout int64
}
func NewHealthChecker(brokers []string, options ...string) *HealthChecker {
	var name string
	if len(options) >= 1 {
		name = options[0]
	} else {
		name = "kafka"
	}
	return NewKafkaHealthChecker(brokers, name, 4)
}

func NewKafkaHealthChecker(brokers []string, name string, timeouts ...int64) *HealthChecker {
	var timeout int64
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	} else {
		timeout = 4
	}
	return &HealthChecker{brokers, name, timeout}
}

func (s *HealthChecker) Name() string {
	return s.Service
}

func (s *HealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	dialer := net.Dialer{
		Timeout: time.Duration(s.Timeout) * time.Second,
	}
	for _, broker := range s.Brokers {
		conn, err := dialer.Dial("tcp", broker)
		if err != nil {
			return res, err
		}
		conn.Close()
	}
	return res, nil
}

func (s *HealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	if data == nil {
		data = make(map[string]interface{}, 0)
	}
	data["error"] = err.Error()
	return data
}
