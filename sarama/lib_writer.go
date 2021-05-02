package kafka

import "github.com/Shopify/sarama"

func MapToHeader(messageAttributes map[string]string) []sarama.RecordHeader {
	headers := make([]sarama.RecordHeader, 0)
	for k, v := range messageAttributes {
		h := sarama.RecordHeader{Key: []byte(k), Value: []byte(v)}
		headers = append(headers, h)
	}
	return headers
}
