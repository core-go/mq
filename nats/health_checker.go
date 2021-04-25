package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"net"
	"time"
)

type HealthChecker struct {
	name    string
	url     string
	timeout time.Duration
}

func NewHttpHealthChecker(name, url string, timeouts ...time.Duration) *HealthChecker {
	var timeout time.Duration
	if len(timeouts) >= 1 {
		timeout = timeouts[0]
	} else {
		timeout = 4 * time.Second
	}
	return &HealthChecker{name: name, url: url, timeout: timeout}
}

func NewHealthChecker(url string, options ...string) *HealthChecker {
	var name string
	if len(options) >= 1 && len(options[0]) > 0 {
		name = options[0]
	} else {
		name = "nats"
	}
	return &HealthChecker{name: name, url: url, timeout: 4 * time.Second}
}

func (s *HealthChecker) Name() string {
	return s.name
}

func (s *HealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	dialer := &net.Dialer{Timeout: s.timeout, DualStack: true}
	opts := &nats.Options{
		Servers: []string{s.url},
		Dialer:  dialer,
	}
	conn, err := opts.Connect()
	if err != nil {
		return nil, err
	}
	conn.Close()
	res["status"] = "success"
	return res, nil
}

func (s *HealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	data["error"] = err.Error()
	return data
}
