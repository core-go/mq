package amq

import (
	"context"
	"github.com/go-stomp/stomp"
)

type HealthChecker struct {
	name string
	addr string
}

func NewHealthChecker(addr string, options ...string) *HealthChecker {
	var name string

	if len(options) >= 2 && len(options[1]) >= 0 {
		name = options[1]
	} else {
		name = "amq"
	}
	return &HealthChecker{name, addr}
}

func (s *HealthChecker) Name() string {
	return s.name
}

func (s *HealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	var conn *stomp.Conn
	
	res := make(map[string]interface{})
	var err error
	conn, err = stomp.Dial("tcp", s.addr)
	if err != nil {
		return res, err
	}

	res["version"] = conn.Version()
	return res, err
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
