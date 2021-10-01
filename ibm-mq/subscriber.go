package ibmmq

import (
	"context"
	"fmt"
	"github.com/core-go/mq"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

type Subscriber struct {
	QueueManager *ibmmq.MQQueueManager
	QueueName    string
	WaitInterval int32
	Topic        string
	LogError     func(context.Context, string)
}

func NewSubscriberByConfig(c SubscriberConfig, auth MQAuth, options ...func(context.Context, string)) (*Subscriber, error) {
	c2 := QueueConfig{
		ManagerName:    c.ManagerName,
		ChannelName:    c.ChannelName,
		ConnectionName: c.ConnectionName,
		QueueName:      c.QueueName,
	}
	mgr, err := newQueueManagerByConfig(c2, auth)
	if err != nil {
		return nil, err
	}
	return NewSubscriber(mgr, c.QueueName, c.Topic, c.WaitInterval, options...), nil
}
func NewSubscriber(mgr *ibmmq.MQQueueManager, topic string, queueName string, waitInterval int32, options ...func(context.Context, string)) *Subscriber {
	sd := ibmmq.NewMQSD()
	sd.Options = ibmmq.MQSO_CREATE |
		ibmmq.MQSO_NON_DURABLE |
		ibmmq.MQSO_MANAGED

	sd.ObjectString = queueName
	return NewSubscriberByMQSD(mgr, queueName, topic, sd, waitInterval, options...)
}
func NewSubscriberByMQSD(manager *ibmmq.MQQueueManager, queueName string, topic string, sd *ibmmq.MQSD, waitInterval int32, options ...func(context.Context, string)) *Subscriber {
	var logError func(context.Context, string)
	if len(options) > 0 {
		logError = options[0]
	}
	return &Subscriber{
		QueueManager: manager,
		QueueName:    queueName,
		WaitInterval: waitInterval,
		Topic:        topic,
		LogError:     logError,
	}
}

func (c *Subscriber) Subscribe(ctx context.Context, handle func(context.Context, *mq.Message, error) error) {
	// The qObject is filled in with a reference to the queue created automatically
	// for publications. It will be used in a moment for the Get operations
	md := ibmmq.NewMQOD()
	openOptions := ibmmq.MQOO_INPUT_SHARED | ibmmq.MQAUTH_INQUIRE

	// Opening a QUEUE (rather than a Topic or other object type) and give the name
	md.ObjectType = ibmmq.MQOT_Q
	md.ObjectName = c.Topic
	qObject, err := c.QueueManager.Open(md, openOptions)
	if err != nil {
		if c.LogError != nil {
			c.LogError(ctx, fmt.Sprintf("Error: %v", err))
		}
		return
	} else {
		defer qObject.Close(0)
	}

	for {
		msgAvail := true
		for msgAvail == true && err == nil {
			mqmd := ibmmq.NewMQMD()
			// The GET requires control structures, the Message Descriptor (MQMD)
			// and Get Options (MQGMO). Create those with default values.
			gmo := ibmmq.NewMQGMO()
			// The default options are OK, but it's always
			// a good idea to be explicit about transactional boundaries as
			// not all platforms behave the same way.
			gmo.Options = ibmmq.MQGMO_NO_SYNCPOINT
			// Set options to wait for a maximum of 3 seconds for any new message to arrive
			gmo.Options |= ibmmq.MQGMO_WAIT // The WaitInterval is in milliseconds
			gmo.WaitInterval = c.WaitInterval
			buffer := make([]byte, 1024)
			l, err := qObject.Get(mqmd, gmo, buffer)

			if err != nil {
				msgAvail = false
				mqReturn := err.(*ibmmq.MQReturn)
				if mqReturn.MQRC != ibmmq.MQRC_NO_MSG_AVAILABLE {
					handle(ctx, nil, err)
				} else {
					err = nil
				}
			} else {
				msgAvail = true
				msg := mq.Message{Data: buffer[:l]}
				handle(ctx, &msg, err)
			}
		}
	}
}
