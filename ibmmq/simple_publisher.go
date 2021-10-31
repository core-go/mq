package ibmmq

import (
	"context"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

type SimplePublisher struct {
	QueueManager *ibmmq.MQQueueManager
}

func NewSimplePublisher(manager *ibmmq.MQQueueManager) *SimplePublisher {
	return &SimplePublisher{manager}
}

func NewSimplePublisherByConfig(c QueueConfig, auth MQAuth) (*SimplePublisher, error) {
	mgr, err := NewQueueManagerByConfig(c, auth)
	if err != nil {
		return nil, err
	}
	return &SimplePublisher{
		QueueManager: mgr,
	}, nil
}

func (p *SimplePublisher) Publish(ctx context.Context, queueName string, data []byte, attributes map[string]string) (string, error) {
	openOptions := ibmmq.MQOO_OUTPUT
	od := ibmmq.NewMQOD()
	od.ObjectType = ibmmq.MQOT_Q
	od.ObjectName = queueName

	topicObject, er1 := p.QueueManager.Open(od, openOptions)
	if er1 != nil {
		return "", er1
	}
	md := ibmmq.NewMQMD()
	pmo := ibmmq.NewMQPMO()

	// The default options are OK, but it's always
	// a good idea to be explicit about transactional boundaries as
	// not all platforms behave the same way.
	pmo.Options = ibmmq.MQPMO_NO_SYNCPOINT

	// Tell MQ what the message body format is. In this case, a text string
	md.Format = ibmmq.MQFMT_STRING

	// Now put the message to the queue
	er2 := topicObject.Put(md, pmo, data)
	return "", er2
}
