package kafka

import (
	"context"
	"github.com/Shopify/sarama"
)

type SimpleReaderHandler struct {
	Topic        []string
	AckOnConsume bool
	Handle       func(context.Context, []byte, map[string]string, error) error
	Convert      func(context.Context, []byte) ([]byte, error)
}

func NewSimpleReaderHandler(Topic []string, handle func(context.Context, []byte, map[string]string, error) error, options...func(context.Context, []byte)([]byte, error)) *SimpleReaderHandler {
	var convert func(context.Context, []byte)([]byte, error)
	if len(options) > 0 {
		convert = options[0]
	}
	return &SimpleReaderHandler{Topic: Topic, AckOnConsume: true, Handle: handle, Convert: convert}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (r *SimpleReaderHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the r as ready
	//close(r.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (r *SimpleReaderHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (r *SimpleReaderHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		attributes := HeaderToMap(msg.Headers)
		if r.AckOnConsume {
			session.MarkMessage(msg, "")
		}
		if r.Convert == nil {
			_ = r.Handle(session.Context(), msg.Value, attributes, nil)
		} else {
			data, err := r.Convert(session.Context(), msg.Value)
			_ = r.Handle(session.Context(), data, attributes, err)
		}
	}
	return nil
}

func HeaderToMap(headers []*sarama.RecordHeader) map[string]string {
	attributes := make(map[string]string, 0)
	for i := range headers {
		attributes[string(headers[i].Key)] = string(headers[i].Value)
	}
	return attributes
}
