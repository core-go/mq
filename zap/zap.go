package zap

import (
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"strings"
	"time"
)

var fieldConfig FieldConfig
var logger *zap.Logger

func SetLogger(logger0 *zap.Logger) {
	logger = logger0
}

func Initialize(c Config) (*zap.Logger, error) {
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
	l, err := NewConfig(c).Build()
	if err == nil {
		logger = l
	}
	return l, err
}

func IsDebugEnable() bool {
	if logger == nil {
		return false
	}
	return logger.Core().Enabled(zapcore.DebugLevel)
}
func IsInfoEnable() bool {
	if logger == nil {
		return false
	}
	return logger.Core().Enabled(zapcore.InfoLevel)
}
func IsWarnEnable() bool {
	if logger == nil {
		return false
	}
	return logger.Core().Enabled(zapcore.WarnLevel)
}
func IsErrorEnable() bool {
	if logger == nil {
		return false
	}
	return logger.Core().Enabled(zapcore.ErrorLevel)
}

func NewConfig(c Config) zap.Config {
	level := zap.NewAtomicLevelAt(zap.InfoLevel)
	if c.Level == "debug" {
		level = zap.NewAtomicLevelAt(zap.DebugLevel)
	} else if c.Level == "warn" {
		level = zap.NewAtomicLevelAt(zap.WarnLevel)
	} else if c.Level == "error" {
		level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	} else if c.Level == "dpanic" {
		level = zap.NewAtomicLevelAt(zap.DPanicLevel)
	} else if c.Level == "panic" {
		level = zap.NewAtomicLevelAt(zap.PanicLevel)
	} else if c.Level == "fatal" {
		level = zap.NewAtomicLevelAt(zap.FatalLevel)
	}
	return zap.Config{
		Level: level,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:         "json",
		EncoderConfig:    NewEncoderConfig(c),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
}
func NewEncoderConfig(c Config) zapcore.EncoderConfig {
	c2 := FieldMap{}
	if c.Map != nil {
		c1 := c.Map
		c2.Time = c1.Time
		c2.Level = c1.Level
		c2.Name = c1.Name
		c2.Caller = c1.Caller
		c2.Msg = c1.Msg
		c2.Stacktrace = c1.Stacktrace
	}
	if len(c2.Time) == 0 {
		c2.Time = "time"
	}
	if len(c2.Level) == 0 {
		c2.Level = "level"
	}
	if len(c2.Name) == 0 {
		c2.Name = "logger"
	}
	if len(c2.Msg) == 0 {
		c2.Msg = "msg"
	}
	if len(c2.Stacktrace) == 0 {
		c2.Stacktrace = "stacktrace"
	}
	return zapcore.EncoderConfig{
		TimeKey:        c2.Time,
		LevelKey:       c2.Level,
		NameKey:        c2.Name,
		CallerKey:      c2.Caller,
		MessageKey:     c2.Msg,
		StacktraceKey:  c2.Stacktrace,
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

func Logger() *zap.Logger {
	return logger
}

func FatalMsg(ctx context.Context, msg string) {
	Fatal(ctx, msg)
}
func PanicMsg(ctx context.Context, msg string) {
	Panic(ctx, msg)
}
func ErrorMsg(ctx context.Context, msg string) {
	ErrorMessage(ctx, msg)
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
func DebugDuration(ctx context.Context, start time.Time, msg interface{}, fs ...zap.Field) {
	if msg == nil || !logger.Core().Enabled(zapcore.DebugLevel) {
		return
	}
	end := time.Now()
	duration := end.Sub(start)
	fields := make([]zap.Field, 0)
	f2 := AppendFields(ctx, fields, fs...)
	f := zap.Int64(fieldConfig.Duration, duration.Milliseconds())
	f2 = append(f2, f)
	s, ok := msg.(string)
	if ok {
		logger.Debug(s, f2...)
	} else {
		b, err := json.Marshal(msg)
		if err == nil {
			s2 := string(b)
			logger.Debug(s2, f2...)
		}
	}
}
func InfoDuration(ctx context.Context, start time.Time, msg interface{}, fs ...zap.Field) {
	if msg == nil || !logger.Core().Enabled(zapcore.InfoLevel) {
		return
	}
	end := time.Now()
	duration := end.Sub(start)
	fields := make([]zap.Field, 0)
	f2 := AppendFields(ctx, fields, fs...)
	f := zap.Int64(fieldConfig.Duration, duration.Milliseconds())
	f2 = append(f2, f)
	s, ok := msg.(string)
	if ok {
		logger.Info(s, f2...)
	} else {
		b, err := json.Marshal(msg)
		if err == nil {
			s2 := string(b)
			logger.Info(s2, f2...)
		}
	}
}
func Debug(ctx context.Context, msg interface{}, fs ...zap.Field) {
	if msg == nil || !logger.Core().Enabled(zapcore.DebugLevel) {
		return
	}
	s, ok := msg.(string)
	if ok {
		DebugMessage(ctx, s, fs...)
	} else {
		b, err := json.Marshal(msg)
		if err == nil {
			s2 := string(b)
			DebugMessage(ctx, s2, fs...)
		}
	}
}
func Info(ctx context.Context, msg interface{}, fs ...zap.Field) {
	if msg == nil || !logger.Core().Enabled(zapcore.InfoLevel) {
		return
	}
	s, ok := msg.(string)
	if ok {
		InfoMessage(ctx, s, fs...)
	} else {
		b, err := json.Marshal(msg)
		if err == nil {
			s2 := string(b)
			InfoMessage(ctx, s2, fs...)
		}
	}
}
func Warn(ctx context.Context, msg interface{}, fs ...zap.Field) {
	if msg == nil || !logger.Core().Enabled(zapcore.WarnLevel) {
		return
	}
	s, ok := msg.(string)
	if ok {
		WarnMessage(ctx, s, fs...)
	} else {
		b, err := json.Marshal(msg)
		if err == nil {
			s2 := string(b)
			WarnMessage(ctx, s2, fs...)
		}
	}
}
func Error(ctx context.Context, msg interface{}, fs ...zap.Field) {
	if msg == nil {
		return
	}
	s, ok := msg.(string)
	if ok {
		ErrorMessage(ctx, s, fs...)
	} else {
		b, err := json.Marshal(msg)
		if err == nil {
			s2 := string(b)
			ErrorMessage(ctx, s2, fs...)
		}
	}
}
func Fatal(ctx context.Context, msg interface{}, fs ...zap.Field) {
	if msg == nil {
		return
	}
	s, ok := msg.(string)
	if ok {
		FatalMessage(ctx, s, fs...)
	} else {
		b, err := json.Marshal(msg)
		if err == nil {
			s2 := string(b)
			FatalMessage(ctx, s2, fs...)
		}
	}
}
func Panic(ctx context.Context, msg interface{}, fs ...zap.Field) {
	if msg == nil {
		return
	}
	s, ok := msg.(string)
	if ok {
		PanicMessage(ctx, s, fs...)
	} else {
		b, err := json.Marshal(msg)
		if err == nil {
			s2 := string(b)
			PanicMessage(ctx, s2, fs...)
		}
	}
}
func DPanic(ctx context.Context, msg interface{}, fs ...zap.Field) {
	if msg == nil {
		return
	}
	s, ok := msg.(string)
	if ok {
		DPanicMessage(ctx, s, fs...)
	} else {
		b, err := json.Marshal(msg)
		if err == nil {
			s2 := string(b)
			DPanicMessage(ctx, s2, fs...)
		}
	}
}

func DebugMessage(ctx context.Context, msg string, fs ...zap.Field) {
	if !logger.Core().Enabled(zapcore.DebugLevel) {
		return
	}
	fields := make([]zap.Field, 0)
	f2 := AppendFields(ctx, fields, fs...)
	logger.Debug(msg, f2...)
}
func InfoMessage(ctx context.Context, msg string, fs ...zap.Field) {
	if !logger.Core().Enabled(zapcore.InfoLevel) {
		return
	}
	fields := make([]zap.Field, 0)
	f2 := AppendFields(ctx, fields, fs...)
	logger.Info(msg, f2...)
}
func WarnMessage(ctx context.Context, msg string, fs ...zap.Field) {
	fields := make([]zap.Field, 0)
	f2 := AppendFields(ctx, fields, fs...)
	logger.Warn(msg, f2...)
}
func ErrorMessage(ctx context.Context, msg string, fs ...zap.Field) {
	fields := make([]zap.Field, 0)
	f2 := AppendFields(ctx, fields, fs...)
	logger.Error(msg, f2...)
}
func FatalMessage(ctx context.Context, msg string, fs ...zap.Field) {
	fields := make([]zap.Field, 0)
	f2 := AppendFields(ctx, fields, fs...)
	logger.Fatal(msg, f2...)
}
func PanicMessage(ctx context.Context, msg string, fs ...zap.Field) {
	fields := make([]zap.Field, 0)
	f2 := AppendFields(ctx, fields, fs...)
	logger.Panic(msg, f2...)
}
func DPanicMessage(ctx context.Context, msg string, fs ...zap.Field) {
	fields := make([]zap.Field, 0)
	f2 := AppendFields(ctx, fields, fs...)
	logger.DPanic(msg, f2...)
}

func AppendFields(ctx context.Context, fields []zap.Field, fs ...zap.Field) []zap.Field {
	l := len(fs)
	for i := 0; i < l; i++ {
		fields = append(fields, fs[i])
	}
	if len(fieldConfig.FieldMap) > 0 {
		if logFields, ok := ctx.Value(fieldConfig.FieldMap).(map[string]string); ok {
			for k, v := range logFields {
				f := zap.String(k, v)
				fields = append(fields, f)
			}
		}
	}
	if fieldConfig.Fields != nil {
		cfs := *fieldConfig.Fields
		for _, k2 := range cfs {
			if v2, ok := ctx.Value(k2).(string); ok && len(v2) > 0 {
				f := zap.String(k2, v2)
				fields = append(fields, f)
			}
		}
	}
	return fields
}
func Debugf(ctx context.Context, format string, args ...interface{}) {
	if !logger.Core().Enabled(zapcore.DebugLevel) {
		return
	}
	msg := fmt.Sprintf(format, args...)
	DebugMessage(ctx, msg)
}
func Infof(ctx context.Context, format string, args ...interface{}) {
	if !logger.Core().Enabled(zapcore.InfoLevel) {
		return
	}
	msg := fmt.Sprintf(format, args...)
	InfoMessage(ctx, msg)
}
func Warnf(ctx context.Context, format string, args ...interface{}) {
	if !logger.Core().Enabled(zapcore.WarnLevel) {
		return
	}
	msg := fmt.Sprintf(format, args...)
	WarnMessage(ctx, msg)
}
func Errorf(ctx context.Context, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	ErrorMessage(ctx, msg)
}
func Panicf(ctx context.Context, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	PanicMessage(ctx, msg)
}

func BuildLogFields(m map[string]interface{}) []zap.Field {
	fields := make([]zap.Field, 0)
	for k, v := range m {
		s, ok := v.(string)
		if ok {
			f := zap.String(k, s)
			fields = append(fields, f)
		} else {
			i2, ok2 := v.(int)
			if ok2 {
				f2 := zap.Int(k, i2)
				fields = append(fields, f2)
			} else {
				i3, ok3 := v.(int64)
				if ok3 {
					f3 := zap.Int64(k, i3)
					fields = append(fields, f3)
				} else {
					i4, ok4 := v.(float64)
					if ok4 {
						f4 := zap.Float64(k, i4)
						fields = append(fields, f4)
					} else {
						msg := fmt.Sprintf("%v", v)
						f5 := zap.String(k, msg)
						fields = append(fields, f5)
					}
				}
			}
		}
	}
	return fields
}

func DebugWithFields(ctx context.Context, msg interface{}, fields map[string]interface{}) {
	if !logger.Core().Enabled(zapcore.DebugLevel) {
		return
	}
	fs := BuildLogFields(fields)
	Debug(ctx, msg, fs...)
}
func DebugfWithFields(ctx context.Context, fields map[string]interface{}, format string, args ...interface{}) {
	if !logger.Core().Enabled(zapcore.DebugLevel) {
		return
	}
	fs := BuildLogFields(fields)
	msg := fmt.Sprintf(format, args...)
	Debug(ctx, msg, fs...)
}
func InfoWithFields(ctx context.Context, msg interface{}, fields map[string]interface{}) {
	if !logger.Core().Enabled(zapcore.InfoLevel) {
		return
	}
	fs := BuildLogFields(fields)
	Info(ctx, msg, fs...)
}
func InfofWithFields(ctx context.Context, fields map[string]interface{}, format string, args ...interface{}) {
	if !logger.Core().Enabled(zapcore.InfoLevel) {
		return
	}
	fs := BuildLogFields(fields)
	msg := fmt.Sprintf(format, args...)
	Info(ctx, msg, fs...)
}
func WarnWithFields(ctx context.Context, msg interface{}, fields map[string]interface{}) {
	if !logger.Core().Enabled(zapcore.WarnLevel) {
		return
	}
	fs := BuildLogFields(fields)
	Warn(ctx, msg, fs...)
}
func WarnfWithFields(ctx context.Context, fields map[string]interface{}, format string, args ...interface{}) {
	if !logger.Core().Enabled(zapcore.WarnLevel) {
		return
	}
	fs := BuildLogFields(fields)
	msg := fmt.Sprintf(format, args...)
	Warn(ctx, msg, fs...)
}
func ErrorWithFields(ctx context.Context, msg interface{}, fields map[string]interface{}) {
	fs := BuildLogFields(fields)
	Error(ctx, msg, fs...)
}
func ErrorfWithFields(ctx context.Context, fields map[string]interface{}, format string, args ...interface{}) {
	fs := BuildLogFields(fields)
	msg := fmt.Sprintf(format, args...)
	Error(ctx, msg, fs...)
}
func FatalWithFields(ctx context.Context, msg interface{}, fields map[string]interface{}) {
	fs := BuildLogFields(fields)
	Fatal(ctx, msg, fs...)
}
func FatalfWithFields(ctx context.Context, fields map[string]interface{}, format string, args ...interface{}) {
	fs := BuildLogFields(fields)
	msg := fmt.Sprintf(format, args...)
	Fatal(ctx, msg, fs...)
}
func PanicWithFields(ctx context.Context, msg interface{}, fields map[string]interface{}) {
	fs := BuildLogFields(fields)
	Panic(ctx, msg, fs...)
}
func PanicfWithFields(ctx context.Context, fields map[string]interface{}, format string, args ...interface{}) {
	fs := BuildLogFields(fields)
	msg := fmt.Sprintf(format, args...)
	Panic(ctx, msg, fs...)
}
func DPanicWithFields(ctx context.Context, msg interface{}, fields map[string]interface{}) {
	fs := BuildLogFields(fields)
	DPanic(ctx, msg, fs...)
}
func DPanicfWithFields(ctx context.Context, fields map[string]interface{}, format string, args ...interface{}) {
	fs := BuildLogFields(fields)
	msg := fmt.Sprintf(format, args...)
	DPanic(ctx, msg, fs...)
}

func DebugFields(ctx context.Context, msg string, fields map[string]interface{}) {
	DebugWithFields(ctx, msg, fields)
}
func InfoFields(ctx context.Context, msg string, fields map[string]interface{}) {
	InfoWithFields(ctx, msg, fields)
}
func WarnFields(ctx context.Context, msg string, fields map[string]interface{}) {
	WarnWithFields(ctx, msg, fields)
}
func ErrorFields(ctx context.Context, msg string, fields map[string]interface{}) {
	ErrorWithFields(ctx, msg, fields)
}
func FatalFields(ctx context.Context, msg string, fields map[string]interface{}) {
	FatalWithFields(ctx, msg, fields)
}
func PanicFields(ctx context.Context, msg string, fields map[string]interface{}) {
	PanicWithFields(ctx, msg, fields)
}
func DPanicFields(ctx context.Context, msg string, fields map[string]interface{}) {
	DPanicWithFields(ctx, msg, fields)
}
