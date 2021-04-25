package amq

import (
	"context"

	"github.com/go-stomp/stomp"
)

type HealthChecker struct {
	Conn *stomp.Conn
	name string
	des  string
}

func NewHealthChecker(conn *stomp.Conn, options ...string) *HealthChecker {
	var name, des string
	if len(options) >= 1 && len(options[0]) >= 0 {
		des = options[0]
	} else {
		des = "Test::Message"
	}
	if len(options) >= 2 && len(options[1]) >= 0 {
		name = options[1]
	} else {
		name = "amq"
	}
	return &HealthChecker{conn, name, des}
}

func (s *HealthChecker) Name() string {
	return s.name
}

func (s *HealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	subscription, err := s.Conn.Subscribe(s.des, stomp.AckAuto,
		stomp.SubscribeOpt.Header("subscription-type", "ANYCAST"),
	)
	if err != nil {
		return res, err
	}
	err = subscription.Unsubscribe()
	res["version"] = s.Conn.Version()
	return res, err
}

func (s *HealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	data["error"] = err.Error()
	return data
}
