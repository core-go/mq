package ibmmq

import (
	"context"
	"fmt"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

type SubscriberConfig struct {
	ManagerName    string `yaml:"manager_name" mapstructure:"manager_name" json:"managerName,omitempty" gorm:"column:managerName" bson:"managerName,omitempty" dynamodbav:"managerName,omitempty" firestore:"managerName,omitempty"`
	ChannelName    string `yaml:"channel_name" mapstructure:"channel_name" json:"channelName,omitempty" gorm:"column:channelname" bson:"channelName,omitempty" dynamodbav:"channelName,omitempty" firestore:"channelName,omitempty"`
	ConnectionName string `yaml:"connection_name" mapstructure:"connection_name" json:"connectionName,omitempty" gorm:"column:connectionname" bson:"connectionName,omitempty" dynamodbav:"connectionName,omitempty" firestore:"connectionName,omitempty"`
	QueueName      string `yaml:"queue_name" mapstructure:"queue_name" json:"queueName,omitempty" gorm:"column:queuename" bson:"queueName,omitempty" dynamodbav:"queueName,omitempty" firestore:"queueName,omitempty"`
	WaitInterval   int32  `yaml:"wait_interval" mapstructure:"wait_interval" json:"waitInterval,omitempty" gorm:"column:waitinterval" bson:"waitInterval,omitempty" dynamodbav:"waitInterval,omitempty" firestore:"waitInterval,omitempty"`
	Topic          string `yaml:"topic" mapstructure:"topic" json:"topic,omitempty" gorm:"column:topic" bson:"topic,omitempty" dynamodbav:"topic,omitempty" firestore:"topic,omitempty"`
}

type Subscriber struct {
	QueueManager *ibmmq.MQQueueManager
	QueueName    string
	WaitInterval int32
	Topic        string
	LogError     func(context.Context, string)
}

func NewSubscriberByConfig(c SubscriberConfig, auth MQAuth, logError func(context.Context, string)) (*Subscriber, error) {
	c2 := QueueConfig{
		ManagerName:    c.ManagerName,
		ChannelName:    c.ChannelName,
		ConnectionName: c.ConnectionName,
		QueueName:      c.QueueName,
	}
	mgr, err := NewQueueManagerByConfig(c2, auth)
	if err != nil {
		return nil, err
	}
	return NewSubscriber(mgr, c.QueueName, c.Topic, c.WaitInterval, logError), nil
}
func NewSubscriber(mgr *ibmmq.MQQueueManager, topic string, queueName string, waitInterval int32, logError func(context.Context, string)) *Subscriber {
	return NewSubscriberByMQSD(mgr, queueName, topic, waitInterval, logError)
}
func NewSubscriberByMQSD(manager *ibmmq.MQQueueManager, queueName string, topic string, waitInterval int32, logError func(context.Context, string)) *Subscriber {
	return &Subscriber{
		QueueManager: manager,
		QueueName:    queueName,
		WaitInterval: waitInterval,
		Topic:        topic,
		LogError:     logError,
	}
}

func (c *Subscriber) Subscribe(ctx context.Context, handle func(context.Context, []byte)) {
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
			gmo.Options |= ibmmq.MQGMO_WAIT
			gmo.WaitInterval = c.WaitInterval // The WaitInterval is in milliseconds
			buffer := make([]byte, 0, 1024)
			buffer, _, err = qObject.GetSlice(mqmd, gmo, buffer)

			if err != nil {
				msgAvail = false
				mqReturn := err.(*ibmmq.MQReturn)
				if mqReturn.MQRC != ibmmq.MQRC_NO_MSG_AVAILABLE {
					c.LogError(ctx, "Error when subscribe: "+err.Error())
				} else {
					err = nil
				}
			} else {
				msgAvail = true
				handle(ctx, buffer)
			}
		}
	}
}
