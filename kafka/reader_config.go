package kafka

import "time"

type ReaderConfig struct {
	Brokers        []string       `mapstructure:"brokers"`
	GroupID        string         `mapstructure:"group_id"`
	Topic          string         `mapstructure:"topic"`
	Client         ClientConfig   `mapstructure:"client"`
	MinBytes       *int           `mapstructure:"min_bytes"`
	MaxBytes       int            `mapstructure:"max_bytes"`
	CommitInterval *time.Duration `mapstructure:"commit_interval"`
}
