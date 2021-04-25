package pubsub

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
)

type PermissionType int

const (
	PermissionPublish   PermissionType = 0
	PermissionSubscribe PermissionType = 1
)

type HealthChecker struct {
	name           string
	client         *pubsub.Client
	timeout        time.Duration
	permissionType PermissionType
	resourceId     string
}

func NewHealthChecker(name string, client *pubsub.Client, resourceId string, permissionType PermissionType, timeout ...time.Duration) *HealthChecker {
	if len(timeout) >= 1 {
		return &HealthChecker{name: name, client: client, permissionType: permissionType, resourceId: resourceId, timeout: timeout[0]}
	}
	return &HealthChecker{name: name, client: client, permissionType: permissionType, resourceId: resourceId, timeout: 4 * time.Second}
}
func NewPubHealthChecker(name string, client *pubsub.Client, resourceId string, timeout ...time.Duration) *HealthChecker {
	if len(timeout) >= 1 {
		return &HealthChecker{name: name, client: client, permissionType: PermissionPublish, resourceId: resourceId, timeout: timeout[0]}
	}
	return &HealthChecker{name: name, client: client, permissionType: PermissionPublish, resourceId: resourceId, timeout: 4 * time.Second}
}
func NewSubHealthChecker(name string, client *pubsub.Client, resourceId string, timeout ...time.Duration) *HealthChecker {
	if len(timeout) >= 1 {
		return &HealthChecker{name: name, client: client, permissionType: PermissionSubscribe, resourceId: resourceId, timeout: timeout[0]}
	}
	return &HealthChecker{name: name, client: client, permissionType: PermissionSubscribe, resourceId: resourceId, timeout: 4 * time.Second}
}
func (h *HealthChecker) Name() string {
	return h.name
}

func (h *HealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	var permissions []string
	var err error

	timeoutCtx, _ := context.WithTimeout(ctx, h.timeout)
	if h.permissionType == PermissionPublish {
		permissions, err = h.client.Topic(h.resourceId).IAM().TestPermissions(timeoutCtx, []string{"pubsub.topics.publish"})
	} else if h.permissionType == PermissionSubscribe {
		permissions, err = h.client.Subscription(h.resourceId).IAM().TestPermissions(timeoutCtx, []string{"pubsub.subscriptions.consume"})
	}

	if err != nil {
		return res, err
	} else if len(permissions) != 1 {
		return res, fmt.Errorf("invalid permissions: %v", permissions)
	} else {
		return res, nil
	}
}

func (h *HealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	data["error"] = err.Error()
	return data
}
