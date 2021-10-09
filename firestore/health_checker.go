package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"google.golang.org/api/option"
	"google.golang.org/api/transport"
)

type HealthChecker struct {
	name        string
	projectId   string
	opts        []option.ClientOption
	credentials []byte
}

func NewFirestoreHealthChecker(name string, projectId string, opts ...option.ClientOption) *HealthChecker {
	return &HealthChecker{projectId: projectId, name: name, opts: opts}
}

func NewHealthCheckerWithProjectId(projectId string, opts ...option.ClientOption) *HealthChecker {
	return NewFirestoreHealthChecker("firestore", projectId, opts...)
}
func NewHealthChecker(ctx context.Context, credentials []byte, options ...string) *HealthChecker {
	var name string
	if len(options) > 0 && len(options[0]) > 0 {
		name = options[0]
	} else {
		name = "firestore"
	}
	opts := option.WithCredentialsJSON(credentials)
	creds, er2 := transport.Creds(ctx, opts)
	if er2 != nil {
		panic("Credentials Error: " + er2.Error())
	}
	if creds == nil {
		panic("Error: creds is nil")
	}
	return NewFirestoreHealthChecker(name, creds.ProjectID, opts)
}
func (s HealthChecker) Name() string {
	return s.name
}

func (s HealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	client, err := firestore.NewClient(ctx, s.projectId, s.opts...)
	if err != nil {
		return res, err
	}
	defer client.Close()
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
