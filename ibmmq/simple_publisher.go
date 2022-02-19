package ibmmq

import (
	"context"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

type SimplePublisher struct {
	QueueManager *ibmmq.MQQueueManager
	Convert func(context.Context, []byte)([]byte, error)
}

func NewSimplePublisher(manager *ibmmq.MQQueueManager, options...func(context.Context, []byte)([]byte, error)) *SimplePublisher {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &SimplePublisher{manager, convert}
}

func NewSimplePublisherByConfig(c QueueConfig, auth MQAuth, options...func(context.Context, []byte)([]byte, error)) (*SimplePublisher, error) {
	mgr, err := NewQueueManagerByConfig(c, auth)
	if err != nil {
		return nil, err
	}
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &SimplePublisher{
		QueueManager: mgr,
		Convert: convert,
	}, nil
}
func (p *SimplePublisher) Put(ctx context.Context, queueName string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, queueName, data, attributes)
}
func (p *SimplePublisher) Send(ctx context.Context, queueName string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, queueName, data, attributes)
}
func (p *SimplePublisher) Write(ctx context.Context, queueName string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, queueName, data, attributes)
}
func (p *SimplePublisher) Produce(ctx context.Context, queueName string, data []byte, attributes map[string]string) (string, error) {
	return p.Publish(ctx, queueName, data, attributes)
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

	var binary = data
	var err error
	if p.Convert != nil {
		binary, err = p.Convert(ctx, data)
		if err != nil {
			return "", err
		}
	}
	// Now put the message to the queue
	er2 := topicObject.Put(md, pmo, binary)
	return "", er2
}
