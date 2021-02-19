package mq

import (
	"context"
)

type MqRetryService struct {
	Produce  func(ctx context.Context, data []byte, attributes map[string]string) (string, error)
	LogError func(context.Context, string)
	LogInfo  func(context.Context, string)
}

func NewMqRetryService(produce func(context.Context, []byte, map[string]string) (string, error), logs ...func(context.Context, string)) *MqRetryService {
	s := &MqRetryService{Produce: produce}
	if len(logs) >= 1 {
		s.LogError = logs[0]
	}
	if len(logs) >= 2 {
		s.LogInfo = logs[1]
	}
	return s
}

func (s *MqRetryService) Retry(ctx context.Context, message *Message) error {
	_, err := s.Produce(ctx, message.Data, message.Attributes)
	if err != nil {
		if s.LogError != nil {
			s.LogError(ctx, `Retry put to mq error: `+err.Error())
		}
	} else if s.LogInfo != nil {
		s.LogInfo(ctx, `Retry put to mq success.`)
	}
	return err
}
