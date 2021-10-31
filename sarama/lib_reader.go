package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/core-go/mq"
)

type ReaderHandler struct {
	Topic        []string
	AckOnConsume bool
	Handle       func(context.Context, *mq.Message, error) error
	Convert       func(context.Context, []byte) ([]byte, error)
}

func NewReaderHandler(Topic []string, handle func(context.Context, *mq.Message, error) error, options...func(context.Context, []byte)([]byte, error)) *ReaderHandler {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &ReaderHandler{Topic: Topic, AckOnConsume: true, Handle: handle, Convert: convert}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (r *ReaderHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the r as ready
	//close(r.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (r *ReaderHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (r *ReaderHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		attributes := HeaderToMap(msg.Headers)
		message := mq.Message{
			Id:         string(msg.Key),
			Data:       msg.Value,
			Attributes: attributes,
			Timestamp:  &msg.Timestamp,
			Raw:        msg,
		}
		if r.AckOnConsume {
			session.MarkMessage(msg, "")
		}
		if r.Convert == nil {
			_ = r.Handle(session.Context(), &message, nil)
		} else {
			data, err := r.Convert(session.Context(), msg.Value)
			if err == nil {
				message.Data = data
			}
			_ = r.Handle(session.Context(), &message, err)
		}

	}
	return nil
}
