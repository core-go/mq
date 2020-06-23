package mq

import (
	"context"
	"github.com/sirupsen/logrus"
)

type MqRetryService struct {
	producer Producer
}

func NewMqRetryService(producer Producer) *MqRetryService {
	return &MqRetryService{producer}
}

func (s *MqRetryService) Retry(ctx context.Context, message *Message) error {
	_, err := s.producer.Produce(ctx, message.Data, &message.Attributes)
	if err != nil {
		logrus.Errorf(`Retry put to mq error: %s`, err.Error())
	} else if logrus.IsLevelEnabled(logrus.DebugLevel){
		logrus.Debug(`Retry put to mq success.`)
	}
	return err
}
