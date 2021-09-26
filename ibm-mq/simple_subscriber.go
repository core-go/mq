package ibmmq

import (
	"context"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

type SubscriberConfig struct {
	ManagerName    string `mapstructure:"manager_name" json:"managerName,omitempty" gorm:"column:managerName" bson:"managerName,omitempty" dynamodbav:"managerName,omitempty" firestore:"managerName,omitempty"`
	ChannelName    string `mapstructure:"channel_name" json:"channelName,omitempty" gorm:"column:channelname" bson:"channelName,omitempty" dynamodbav:"channelName,omitempty" firestore:"channelName,omitempty"`
	ConnectionName string `mapstructure:"connection_name" json:"connectionName,omitempty" gorm:"column:connectionname" bson:"connectionName,omitempty" dynamodbav:"connectionName,omitempty" firestore:"connectionName,omitempty"`
	QueueName      string `mapstructure:"queue_name" json:"queueName,omitempty" gorm:"column:queuename" bson:"queueName,omitempty" dynamodbav:"queueName,omitempty" firestore:"queueName,omitempty"`
	WaitInterval   int32  `mapstructure:"wait_interval" json:"waitInterval,omitempty" gorm:"column:waitinterval" bson:"waitInterval,omitempty" dynamodbav:"waitInterval,omitempty" firestore:"waitInterval,omitempty"`
}

type SimpleSubscriber struct {
	QueueManager *ibmmq.MQQueueManager
	QueueName    string
	sd           *ibmmq.MQSD
	md           *ibmmq.MQMD
	gmo          *ibmmq.MQGMO
}

func NewSimpleSubscriberByConfig(c SubscriberConfig, auth MQAuth) (*SimpleSubscriber, error) {
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
	return NewSimpleSubscriber(mgr, c.QueueName, c.WaitInterval), nil
}
func NewSimpleSubscriber(mgr *ibmmq.MQQueueManager, queueName string, waitInterval int32) *SimpleSubscriber {
	sd := ibmmq.NewMQSD()
	sd.Options = ibmmq.MQSO_CREATE |
		ibmmq.MQSO_NON_DURABLE |
		ibmmq.MQSO_MANAGED

	sd.ObjectString = queueName
	return NewSimpleSubscriberByMQSD(mgr, queueName, sd, waitInterval)
}
func NewSimpleSubscriberByMQSD(manager *ibmmq.MQQueueManager, queueName string, sd *ibmmq.MQSD, waitInterval int32) *SimpleSubscriber {
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
	return &SimpleSubscriber{QueueManager: manager, QueueName: queueName, sd: sd, md: md, gmo: gmo}
}

func (c *SimpleSubscriber) Subscribe(ctx context.Context, handle func(context.Context, []byte, map[string]string, error) error) {
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
				handle(ctx, nil, nil, err)
			} else {
				// If there's no message available, then I won't treat that as a real error as
				// it's an expected situation
				err = nil
			}
		} else {
			msgAvail = true
			handle(ctx, buffer, nil, err)
		}
	}
}
