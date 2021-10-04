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
	queue        *QueueConfig
	auth         *MQAuth
}

var qObject ibmmq.MQObject

func NewHealthCheckerByConfig(queue *QueueConfig, auth *MQAuth, topic string, options ...string) *HealthChecker {
	var name string
	if len(options) >= 1 {
		name = options[0]
	} else {
		name = "ibmmq"
	}
	return NewIBMMQHealthCheckerByConfig(queue, auth, topic, name, 4*time.Second)
}
func NewHealthChecker(connection *ibmmq.MQQueueManager, topic string, options ...string) *HealthChecker {
	var name string
	if len(options) >= 1 {
		name = options[0]
	} else {
		name = "ibmmq"
	}
	return NewHealthCheckerWithTimeout(connection, topic, name, 4*time.Second)
}

func NewHealthCheckerWithTimeout(connection *ibmmq.MQQueueManager, topic string, name string, timeouts ...time.Duration) *HealthChecker {
	var timeout time.Duration
	if len(timeouts) >= 1 {
		timeout = timeouts[0]
	} else {
		timeout = 4 * time.Second
	}
	return &HealthChecker{name: name, queueManager: connection, topic: topic, timeout: timeout}
}

func NewIBMMQHealthCheckerByConfig(queue *QueueConfig, auth *MQAuth, topic string, name string, timeouts ...time.Duration) *HealthChecker {
	var timeout time.Duration
	if len(timeouts) >= 1 {
		timeout = timeouts[0]
	} else {
		timeout = 4 * time.Second
	}
	return &HealthChecker{name: name, queue: queue, auth: auth, topic: topic, timeout: timeout}
}

func (s *HealthChecker) Name() string {
	return s.name
}

func (s *HealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	if s.queueManager == nil {
		conn, err := NewQueueManagerByConfig(*s.queue, *s.auth)
		if err != nil {
			return res, err
		}
		s.queueManager = conn
	}
	sd := ibmmq.NewMQSD()
	sd.Options = ibmmq.MQSO_CREATE |
		ibmmq.MQSO_NON_DURABLE |
		ibmmq.MQSO_MANAGED
	sd.ObjectString = s.topic
	subscriptionObject, err := s.queueManager.Sub(sd, &qObject)
	if err != nil {
		return res, err
	}
	err = subscriptionObject.Close(0)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (s *HealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	if data == nil {
		return make(map[string]interface{}, 0)
	}
	data["error"] = err.Error()
	return data
}