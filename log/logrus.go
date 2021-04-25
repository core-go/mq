package log

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"strings"
	"time"
)

var fieldConfig FieldConfig
var logger *logrus.Logger

func Initialize(c Config) *logrus.Logger {
	fieldConfig.FieldMap = c.FieldMap
	if len(c.Duration) > 0 {
		fieldConfig.Duration = c.Duration
	} else {
		fieldConfig.Duration = "duration"
	}
	if len(c.Fields) > 0 {
		fields := strings.Split(c.Fields, ",")
		fieldConfig.Fields = &fields
	}
	l := logrus.New()

	// kibana: time:@timestamp msg:message
	formatter := logrus.JSONFormatter{TimestampFormat: "2006-01-02T15:04:05.000Z0700"}
	if len(c.TimestampFormat) > 0 {
		formatter.TimestampFormat = c.TimestampFormat
	}
	if c.Map != nil {
		formatter.FieldMap = *c.Map
	}
	x := &formatter
	l.SetFormatter(x)
	logrus.SetFormatter(x)
	if len(c.Level) > 0 {
		if level, err := logrus.ParseLevel(c.Level); err == nil {
			l.SetLevel(level)
			logrus.SetLevel(level)
		} else {
			logrus.Errorf("Can't parse LOG_LEVEL: %s.", c.Level)
		}
	}
	logger = l
	return l
}

func IsTraceEnable() bool {
	return logrus.IsLevelEnabled(logrus.TraceLevel)
}
func IsDebugEnable() bool {
	return logrus.IsLevelEnabled(logrus.DebugLevel)
}
func IsInfoEnable() bool {
	return logrus.IsLevelEnabled(logrus.InfoLevel)
}
func IsWarnEnable() bool {
	return logrus.IsLevelEnabled(logrus.WarnLevel)
}
func IsErrorEnable() bool {
	return logrus.IsLevelEnabled(logrus.ErrorLevel)
}

func Key(logger *logrus.Logger) map[string]string {
	f1, ok := logger.Formatter.(*logrus.JSONFormatter)
	if ok {
		maps := make(map[string]string)
		fms := f1.FieldMap
		for k, e := range fms {
			k2 := fmt.Sprint(k)
			maps[k2] = e
		}
		return maps
	}
	return nil
}

func Logger() *logrus.Logger {
	return logger
}

func FatalMsg(ctx context.Context, msg string) {
	Fatal(ctx, msg)
}
func PanicMsg(ctx context.Context, msg string) {
	Panic(ctx, msg)
}
func ErrorMsg(ctx context.Context, msg string) {
	Error(ctx, msg)
}
func WarnMsg(ctx context.Context, msg string) {
	Warn(ctx, msg)
}
func InfoMsg(ctx context.Context, msg string) {
	Info(ctx, msg)
}
func DebugMsg(ctx context.Context, msg string) {
	Info(ctx, msg)
}
func TraceMsg(ctx context.Context, msg string) {
	Trace(ctx, msg)
}
func DebugDuration(ctx context.Context, start time.Time, args ...interface{}) {
	LogDuration(ctx, logrus.DebugLevel, start, args)
}
func InfoDuration(ctx context.Context, start time.Time, args ...interface{}) {
	LogDuration(ctx, logrus.InfoLevel, start, args)
}
func LogDuration(ctx context.Context, level logrus.Level, start time.Time, args ...interface{}) {
	if logrus.IsLevelEnabled(level) {
		end := time.Now()
		duration := end.Sub(start)
		fields := AppendFields(ctx, logrus.Fields{})
		fields[fieldConfig.Duration] = duration.Milliseconds()
		logrus.WithFields(fields).Log(level, args...)
	}
}
func LogfDuration(ctx context.Context, level logrus.Level, start time.Time, format string, args ...interface{}) {
	if logrus.IsLevelEnabled(level) {
		end := time.Now()
		duration := end.Sub(start)
		fields := AppendFields(ctx, logrus.Fields{})
		fields[fieldConfig.Duration] = duration.Milliseconds()
		logrus.WithFields(fields).Logf(level, format, args...)
	}
}

func Log(ctx context.Context, level logrus.Level, args ...interface{}) {
	if logrus.IsLevelEnabled(level) {
		fields := AppendFields(ctx, logrus.Fields{})
		if len(args) == 1 {
			msg := args[0]
			s1, ok := msg.(string)
			if ok {
				logrus.WithFields(fields).Log(level, s1)
			} else {
				bs, err := json.Marshal(msg)
				if err != nil {
					logrus.WithFields(fields).Log(level, args...)
				} else {
					s2 := string(bs)
					logrus.WithFields(fields).Log(level, s2)
				}
			}
		} else {
			logrus.WithFields(fields).Log(level, args...)
		}
	}
}
func Logf(ctx context.Context, level logrus.Level, format string, args ...interface{}) {
	if logrus.IsLevelEnabled(level) {
		fields := AppendFields(ctx, logrus.Fields{})
		logrus.WithFields(fields).Logf(level, format, args...)
	}
}

func Trace(ctx context.Context, args ...interface{}) {
	Log(ctx, logrus.TraceLevel, args...)
}
func Debug(ctx context.Context, args ...interface{}) {
	Log(ctx, logrus.DebugLevel, args...)
}
func Info(ctx context.Context, args ...interface{}) {
	Log(ctx, logrus.InfoLevel, args...)
}
func Warn(ctx context.Context, args ...interface{}) {
	Log(ctx, logrus.WarnLevel, args...)
}
func Error(ctx context.Context, args ...interface{}) {
	Log(ctx, logrus.ErrorLevel, args...)
}
func Fatal(ctx context.Context, args ...interface{}) {
	Log(ctx, logrus.FatalLevel, args...)
}
func Panic(ctx context.Context, args ...interface{}) {
	Log(ctx, logrus.PanicLevel, args...)
}

func AppendFields(ctx context.Context, fields logrus.Fields) logrus.Fields {
	if len(fieldConfig.FieldMap) > 0 {
		if logFields, ok := ctx.Value(fieldConfig.FieldMap).(map[string]interface{}); ok {
			for k, v := range logFields {
				fields[k] = v
			}
		}
	}
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

func Tracef(ctx context.Context, format string, args ...interface{}) {
	Logf(ctx, logrus.TraceLevel, format, args...)
}
func Debugf(ctx context.Context, format string, args ...interface{}) {
	Logf(ctx, logrus.DebugLevel, format, args...)
}
func Infof(ctx context.Context, format string, args ...interface{}) {
	Logf(ctx, logrus.InfoLevel, format, args...)
}
func Warnf(ctx context.Context, format string, args ...interface{}) {
	Logf(ctx, logrus.WarnLevel, format, args...)
}
func Errorf(ctx context.Context, format string, args ...interface{}) {
	Logf(ctx, logrus.ErrorLevel, format, args...)
}
func Fatalf(ctx context.Context, format string, args ...interface{}) {
	Logf(ctx, logrus.FatalLevel, format, args...)
}
func Panicf(ctx context.Context, format string, args ...interface{}) {
	Logf(ctx, logrus.PanicLevel, format, args...)
}

func BuildLogFields(m map[string]interface{}) logrus.Fields {
	logFields := logrus.Fields{}
	for k, v := range m {
		logFields[k] = v
	}
	return logFields
}
func LogWithFields(ctx context.Context, level logrus.Level, msg interface{}, fields map[string]interface{}) {
	if msg == nil {
		return
	}
	if logrus.IsLevelEnabled(level) {
		fs := BuildLogFields(fields)
		fs2 := AppendFields(ctx, fs)
		s1, ok := msg.(string)
		if ok {
			logrus.WithFields(fs2).Log(level, s1)
		} else {
			bs, err := json.Marshal(msg)
			if err != nil {
				logrus.WithFields(fs2).Log(level, msg)
			} else {
				s2 := string(bs)
				logrus.WithFields(fs2).Log(level, s2)
			}
		}
	}
}
func LogfWithFields(ctx context.Context, level logrus.Level, fields map[string]interface{}, format string, args ...interface{}) {
	if logrus.IsLevelEnabled(level) {
		fs := BuildLogFields(fields)
		fs2 := AppendFields(ctx, fs)
		msg := fmt.Sprintf(format, args...)
		logrus.WithFields(fs2).Log(level, msg)
	}
}

func DebugWithFields(ctx context.Context, msg interface{}, fields map[string]interface{}) {
	LogWithFields(ctx, logrus.DebugLevel, msg, fields)
}
func DebugfWithFields(ctx context.Context, fields map[string]interface{}, format string, args ...interface{}) {
	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		msg := fmt.Sprintf(format, args...)
		LogWithFields(ctx, logrus.DebugLevel, msg, fields)
	}
}
func InfoWithFields(ctx context.Context, msg interface{}, fields map[string]interface{}) {
	LogWithFields(ctx, logrus.InfoLevel, msg, fields)
}
func InfofWithFields(ctx context.Context, fields map[string]interface{}, format string, args ...interface{}) {
	if logrus.IsLevelEnabled(logrus.InfoLevel) {
		msg := fmt.Sprintf(format, args...)
		LogWithFields(ctx, logrus.InfoLevel, msg, fields)
	}
}
func WarnWithFields(ctx context.Context, msg interface{}, fields map[string]interface{}) {
	LogWithFields(ctx, logrus.WarnLevel, msg, fields)
}
func WarnfWithFields(ctx context.Context, fields map[string]interface{}, format string, args ...interface{}) {
	if logrus.IsLevelEnabled(logrus.WarnLevel) {
		msg := fmt.Sprintf(format, args...)
		LogWithFields(ctx, logrus.WarnLevel, msg, fields)
	}
}
func ErrorWithFields(ctx context.Context, msg interface{}, fields map[string]interface{}) {
	LogWithFields(ctx, logrus.ErrorLevel, msg, fields)
}
func ErrorfWithFields(ctx context.Context, fields map[string]interface{}, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	LogWithFields(ctx, logrus.ErrorLevel, msg, fields)
}
func FatalWithFields(ctx context.Context, msg interface{}, fields map[string]interface{}) {
	LogWithFields(ctx, logrus.FatalLevel, msg, fields)
}
func FatalfWithFields(ctx context.Context, fields map[string]interface{}, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	LogWithFields(ctx, logrus.FatalLevel, msg, fields)
}
func PanicWithFields(ctx context.Context, msg interface{}, fields map[string]interface{}) {
	LogWithFields(ctx, logrus.PanicLevel, msg, fields)
}
func PanicfWithFields(ctx context.Context, fields map[string]interface{}, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	LogWithFields(ctx, logrus.PanicLevel, msg, fields)
}

func TraceFields(ctx context.Context, msg string, fields map[string]interface{}) {
	LogWithFields(ctx, logrus.TraceLevel, msg, fields)
}
func DebugFields(ctx context.Context, msg string, fields map[string]interface{}) {
	LogWithFields(ctx, logrus.DebugLevel, msg, fields)
}
func InfoFields(ctx context.Context, msg string, fields map[string]interface{}) {
	LogWithFields(ctx, logrus.InfoLevel, msg, fields)
}
func WarnFields(ctx context.Context, msg string, fields map[string]interface{}) {
	LogWithFields(ctx, logrus.WarnLevel, msg, fields)
}
func ErrorFields(ctx context.Context, msg string, fields map[string]interface{}) {
	LogWithFields(ctx, logrus.ErrorLevel, msg, fields)
}
func FatalFields(ctx context.Context, msg string, fields map[string]interface{}) {
	LogWithFields(ctx, logrus.FatalLevel, msg, fields)
}
func PanicFields(ctx context.Context, msg string, fields map[string]interface{}) {
	LogWithFields(ctx, logrus.PanicLevel, msg, fields)
}
