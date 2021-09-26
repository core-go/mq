package mq

import "time"

func CreateLog(data []byte, header map[string]string, id string, timestamp *time.Time) interface{} {
	if len(id) == 0 && timestamp == nil && (header == nil || len(header) == 0) {
		return data
	}
	m := make(map[string]interface{})
	m["data"] = data
	if header != nil && len(header) > 0 {
		m["attributes"] = header
	}
	if len(id) > 0 {
		m["id"] = id
	}
	if timestamp != nil {
		m["timestamp"] = timestamp
	}
	return m
}
