package mq

import "context"

type RetryService struct {
	send  func(ctx context.Context, data []byte, attributes map[string]string) (string, error)
	LogError func(context.Context, string)
	LogInfo  func(context.Context, string)
}

func NewRetryService(send func(context.Context, []byte, map[string]string) (string, error), logs ...func(context.Context, string)) *RetryService {
	s := &RetryService{send: send}
	if len(logs) >= 1 {
		s.LogError = logs[0]
	}
	if len(logs) >= 2 {
		s.LogInfo = logs[1]
	}
	return s
}

func (s *RetryService) Retry(ctx context.Context, message *Message) error {
	_, err := s.send(ctx, message.Data, message.Attributes)
	if err != nil {
		if s.LogError != nil {
			s.LogError(ctx, `Retry put to mq error: `+err.Error())
		}
	} else if s.LogInfo != nil {
		s.LogInfo(ctx, `Retry put to mq success.`)
	}
	return err
}
