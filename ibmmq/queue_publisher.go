package ibmmq

import (
	"context"

	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

type QueuePublisher struct {
	QueueManager *ibmmq.MQQueueManager
}

func NewQueuePublisher(manager *ibmmq.MQQueueManager) *QueuePublisher {
	return &QueuePublisher{manager}
}

func NewQueuePublisherByConfig(c QueueConfig, auth MQAuth) (*QueuePublisher, error) {
	mgr, err := NewQueueManagerByConfig(c, auth)
	if err != nil {
		return nil, err
	}
	return &QueuePublisher{QueueManager: mgr}, nil
}
func (p *QueuePublisher) Publish(ctx context.Context, queueName string, data []byte) error {
	openOptions := ibmmq.MQOO_OUTPUT
	od := ibmmq.NewMQOD()
	od.ObjectType = ibmmq.MQOT_Q
	od.ObjectName = queueName

	topicObject, err := p.QueueManager.Open(od, openOptions)
	if err != nil {
		return err
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
	return topicObject.Put(md, pmo, data)
}
