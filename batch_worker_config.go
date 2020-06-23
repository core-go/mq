package mq

type BatchWorkerConfig struct {
	BatchSize  int   `mapstructure:"batch_size"`
	Timeout    int64 `mapstructure:"timeout"`
	LimitRetry int   `mapstructure:"limit_retry"`
}
