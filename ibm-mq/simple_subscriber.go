package ibmmq

import (
	"context"
	"fmt"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

type SubscriberConfig struct {
	ManagerName    string `mapstructure:"manager_name" json:"managerName,omitempty" gorm:"column:managerName" bson:"managerName,omitempty" dynamodbav:"managerName,omitempty" firestore:"managerName,omitempty"`
	ChannelName    string `mapstructure:"channel_name" json:"channelName,omitempty" gorm:"column:channelname" bson:"channelName,omitempty" dynamodbav:"channelName,omitempty" firestore:"channelName,omitempty"`
	ConnectionName string `mapstructure:"connection_name" json:"connectionName,omitempty" gorm:"column:connectionname" bson:"connectionName,omitempty" dynamodbav:"connectionName,omitempty" firestore:"connectionName,omitempty"`
	QueueName      string `mapstructure:"queue_name" json:"queueName,omitempty" gorm:"column:queuename" bson:"queueName,omitempty" dynamodbav:"queueName,omitempty" firestore:"queueName,omitempty"`
	WaitInterval   int32  `mapstructure:"wait_interval" json:"waitInterval,omitempty" gorm:"column:waitinterval" bson:"waitInterval,omitempty" dynamodbav:"waitInterval,omitempty" firestore:"waitInterval,omitempty"`
	Topic          string `mapstructure:"topic" json:"topic,omitempty" gorm:"column:topic" bson:"topic,omitempty" dynamodbav:"topic,omitempty" firestore:"topic,omitempty"`
}

type SimpleSubscriber struct {
	QueueManager *ibmmq.MQQueueManager
	QueueName    string
	sd           *ibmmq.MQSD
	md           *ibmmq.MQMD
	gmo          *ibmmq.MQGMO
	WaitInterval int32
	Topic        string
	LogError     func(context.Context, string)
}

func NewSimpleSubscriberByConfig(c SubscriberConfig, auth MQAuth, options ...func(context.Context, string)) (*SimpleSubscriber, error) {
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
	return NewSimpleSubscriber(mgr, c.QueueName, c.Topic, c.WaitInterval, options...), nil
}
func NewSimpleSubscriber(mgr *ibmmq.MQQueueManager, topic string, queueName string, waitInterval int32, options ...func(context.Context, string)) *SimpleSubscriber {
	sd := ibmmq.NewMQSD()
	sd.Options = ibmmq.MQSO_CREATE |
		ibmmq.MQSO_NON_DURABLE |
		ibmmq.MQSO_MANAGED

	sd.ObjectString = queueName
	return NewSimpleSubscriberByMQSD(mgr, queueName, topic, sd, waitInterval, options...)
}
func NewSimpleSubscriberByMQSD(manager *ibmmq.MQQueueManager, queueName string, topic string, sd *ibmmq.MQSD, waitInterval int32, options ...func(context.Context, string)) *SimpleSubscriber {
	var logError func(context.Context, string)
	if len(options) > 0 {
		logError = options[0]
	}
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
	return &SimpleSubscriber{
		QueueManager: manager,
		QueueName:    queueName,
		sd:           sd,
		md:           md,
		gmo:          gmo,
		WaitInterval: waitInterval,
		Topic:        topic,
		LogError:     logError,
	}
}

func (c *SimpleSubscriber) Subscribe(ctx context.Context, handle func(context.Context, []byte, map[string]string, error) error) {
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
			gmo := ibmmq.NewMQGMO()

			gmo.Options = ibmmq.MQGMO_NO_SYNCPOINT
			gmo.Options |= ibmmq.MQGMO_WAIT
			gmo.WaitInterval = c.WaitInterval
			buffer := make([]byte, 1024)
			l, err := qObject.Get(mqmd, gmo, buffer)

			if err != nil {
				msgAvail = false
				mqReturn := err.(*ibmmq.MQReturn)
				if mqReturn.MQRC != ibmmq.MQRC_NO_MSG_AVAILABLE {
					handle(ctx, nil, nil, err)
				} else {
					err = nil
				}
			} else {
				msgAvail = true
				handle(ctx, buffer[:l], nil, err)
			}
		}
	}
}
