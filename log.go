package mq

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"strings"
)

type iFieldConfig struct {
	Fields *[]string `mapstructure:"fields"`
}

var fieldConfig iFieldConfig

func SetLogFields(fields string) {
	if len(fields) > 0 {
		fields := strings.Split(fields, ",")
		fieldConfig.Fields = &fields
	}
}
func SetFields(fields ...string) {
	fieldConfig.Fields = &fields
}
func IsDebugEnabled() bool {
	return logrus.IsLevelEnabled(logrus.DebugLevel)
}
func IsInfoEnabled() bool {
	return logrus.IsLevelEnabled(logrus.InfoLevel)
}
func Debug(ctx context.Context, msg string) {
	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		logFields := logrus.Fields{}
		f2 := AppendFields(ctx, logFields)
		logrus.WithFields(f2).Debug(msg)
	}
}
func Info(ctx context.Context, msg string) {
	if logrus.IsLevelEnabled(logrus.InfoLevel) {
		logFields := logrus.Fields{}
		f2 := AppendFields(ctx, logFields)
		logrus.WithFields(f2).Info(msg)
	}
}
func Error(ctx context.Context, msg string) {
	logFields := logrus.Fields{}
	f2 := AppendFields(ctx, logFields)
	logrus.WithFields(f2).Error(msg)
}
func Debugf(ctx context.Context, format string, args ...interface{}) {
	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		msg := fmt.Sprintf(format, args...)
		Debug(ctx, msg)
	}
}
func Infof(ctx context.Context, format string, args ...interface{}) {
	if logrus.IsLevelEnabled(logrus.InfoLevel) {
		msg := fmt.Sprintf(format, args...)
		Info(ctx, msg)
	}
}
func Errorf(ctx context.Context, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	Error(ctx, msg)
}
func AppendFields(ctx context.Context, fields logrus.Fields) logrus.Fields {
	if fieldConfig.Fields != nil {
		cfs := *fieldConfig.Fields
		for _, k2 := range cfs {
			if v2, ok := ctx.Value(k2).(string); ok && len(v2) > 0 {
				fields[k2] = v2
			}
		}
	}
	return fields
}
