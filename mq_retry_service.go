package mq

import (
	"context"
)

type MqRetryService struct {
	Producer Producer
	LogError func(context.Context, string)
	LogInfo  func(context.Context, string)
}

func NewMqRetryService(producer Producer, logs ...func(context.Context, string)) *MqRetryService {
	s := &MqRetryService{Producer: producer}
	if len(logs) >= 1 {
		s.LogError = logs[0]
	}
	if len(logs) >= 2 {
		s.LogInfo = logs[1]
	}
	return s
}

func (s *MqRetryService) Retry(ctx context.Context, message *Message) error {
	_, err := s.Producer.Produce(ctx, message.Data, message.Attributes)
	if err != nil {
		if s.LogError != nil {
			s.LogError(ctx, `Retry put to mq error: `+err.Error())
		}
	} else if s.LogInfo != nil {
		s.LogInfo(ctx, `Retry put to mq success.`)
	}
	return err
}
