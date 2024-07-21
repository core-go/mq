package kafka

import (
	"context"
	"github.com/IBM/sarama"
)

type ConsumerHandler struct {
	Topic        []string
	AckOnConsume bool
	Handle       func(context.Context, []byte, map[string]string)
}

func NewConsumerHandler(Topic []string, handle func(context.Context, []byte, map[string]string), ackOnConsume bool) *ConsumerHandler {
	return &ConsumerHandler{Topic: Topic, AckOnConsume: ackOnConsume, Handle: handle}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (r *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the r as ready
	//close(r.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (r *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (r *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		attributes := HeaderToMap(msg.Headers)
		if r.AckOnConsume {
			session.MarkMessage(msg, "")
		}
		r.Handle(session.Context(), msg.Value, attributes)
	}
	return nil
}

func HeaderToMap(headers []*sarama.RecordHeader) map[string]string {
	attributes := make(map[string]string)
	for i := range headers {
		attributes[string(headers[i].Key)] = string(headers[i].Value)
	}
	return attributes
}
