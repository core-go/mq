package rabbitmq

import "context"

type HealthChecker struct {
	url  string
	name string
}

func NewHealthChecker(url string, options ...string) *HealthChecker {
	var name string
	if len(options) > 0 && len(options[0]) > 0 {
		name = options[0]
	} else {
		name = "rabbitmq"
	}
	return &HealthChecker{url, name}
}

func (s *HealthChecker) Name() string {
	return s.name
}

func (s *HealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	channel, er1 := NewChannel(s.url)
	if er1 != nil {
		return res, er1
	}
	er2 := channel.Close()
	return res, er2
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
