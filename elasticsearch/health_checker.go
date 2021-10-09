package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch/v7"
)

type HealthChecker struct {
	client *elasticsearch.Client
	name   string
}

func NewHealthChecker(client *elasticsearch.Client, options ...string) *HealthChecker {
	var name string
	if len(options) > 0 && len(options[0]) > 0 {
		name = options[0]
	} else {
		name = "elasticsearch"
	}
	return &HealthChecker{client, name}
}

func (e *HealthChecker) Name() string {
	return e.name
}

func (e *HealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	_, err := e.client.Ping()
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *HealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	if data == nil {
		data = make(map[string]interface{}, 0)
	}
	data["error"] = err.Error()
	return data
}
