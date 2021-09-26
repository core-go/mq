package ibmmq

import (
	"context"
	"github.com/core-go/mq"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

type Subscriber struct {
	QueueManager *ibmmq.MQQueueManager
	QueueName    string
	sd           *ibmmq.MQSD
	md           *ibmmq.MQMD
	gmo          *ibmmq.MQGMO
}

func NewSubscriberByConfig(c SubscriberConfig, auth MQAuth) (*Subscriber, error) {
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
	return NewSubscriber(mgr, c.QueueName, c.WaitInterval), nil
}
func NewSubscriber(mgr *ibmmq.MQQueueManager, queueName string, waitInterval int32) *Subscriber {
	sd := ibmmq.NewMQSD()
	sd.Options = ibmmq.MQSO_CREATE |
		ibmmq.MQSO_NON_DURABLE |
		ibmmq.MQSO_MANAGED

	sd.ObjectString = queueName
	return NewSubscriberByMQSD(mgr, queueName, sd, waitInterval)
}
func NewSubscriberByMQSD(manager *ibmmq.MQQueueManager, queueName string, sd *ibmmq.MQSD, waitInterval int32) *Subscriber {
	md := ibmmq.NewMQMD()

	// The GET requires control structures, the Message Descriptor (MQMD)
	// and Get Options (MQGMO). Create those with default values.
	gmo := ibmmq.NewMQGMO()
	// The default options are OK, but it's always
	// a good idea to be explicit about transactional boundaries as
	// not all platforms behave the same way.
	gmo.Options = ibmmq.MQGMO_NO_SYNCPOINT
	// Set options to wait for a maximum of 3 seconds for any new message to arrive
	gmo.Options |= ibmmq.MQGMO_WAIT
	gmo.WaitInterval = waitInterval // The WaitInterval is in milliseconds
	return &Subscriber{QueueManager: manager, QueueName: queueName, sd: sd, md: md, gmo: gmo}
}

func (c *Subscriber) Subscribe(ctx context.Context, handle func(context.Context, *mq.Message, error) error) {
	// Create the Object Descriptor that allows us to give the topic name

	var qObjectForC ibmmq.MQObject
	// The qObject is filled in with a reference to the queue created automatically
	// for publications. It will be used in a moment for the Get operations
	_, err := c.QueueManager.Sub(c.sd, &qObjectForC)

	msgAvail := true
	for msgAvail == true && err == nil {
		// Create a buffer for the message data. This one is large enough
		// for the messages put by the amqsput sample.
		buffer := make([]byte, 1024)
		_, err = qObjectForC.Get(c.md, c.gmo, buffer)

		if err != nil {
			msgAvail = false
			mqReturn := err.(*ibmmq.MQReturn)
			if mqReturn.MQRC != ibmmq.MQRC_NO_MSG_AVAILABLE {
				handle(ctx, nil, err)
			} else {
				// If there's no message available, then I won't treat that as a real error as
				// it's an expected situation
				err = nil
			}
		} else {
			msgAvail = true
			msg := mq.Message{
				Data: buffer,
			}
			handle(ctx, &msg, err)
		}
	}
}
