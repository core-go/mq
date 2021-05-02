package ibmmq

import (
	"context"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	"time"
)

type HealthChecker struct {
	name         string
	queueManager *ibmmq.MQQueueManager
	topic        string
	timeout      time.Duration
}

var qObject ibmmq.MQObject

func NewHealthChecker(connection *ibmmq.MQQueueManager, topic string, options ...string) *HealthChecker {
	var name string
	if len(options) >= 1 {
		name = options[0]
	} else {
		name = "ibmmq"
	}
	return NewIBMMQHealthChecker(connection, topic, name, 4*time.Second)
}

func NewIBMMQHealthChecker(connection *ibmmq.MQQueueManager, topic string, name string, timeouts ...time.Duration) *HealthChecker {
	var timeout time.Duration
	if len(timeouts) >= 1 {
		timeout = timeouts[0]
	} else {
		timeout = 4 * time.Second
	}
	return &HealthChecker{name, connection, topic, timeout}
}

func (s *HealthChecker) Name() string {
	return s.name
}

func (s *HealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	sd := ibmmq.NewMQSD()
	sd.Options = ibmmq.MQSO_CREATE |
		ibmmq.MQSO_NON_DURABLE |
		ibmmq.MQSO_MANAGED
	sd.ObjectString = s.topic
	subscriptionObject, err := s.queueManager.Sub(sd, &qObject)
	if err != nil {
		return nil, err
	}
	err = subscriptionObject.Close(0)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *HealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	data["error"] = err.Error()
	return data
}
