package log

import (
	"errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"path/filepath"
)

func NewWriter(ws zapcore.WriteSyncer, conf zap.Config) zap.Option {
	var enc zapcore.Encoder
	switch conf.Encoding {
	case "json":
		enc = zapcore.NewJSONEncoder(conf.EncoderConfig)
	case "console":
		enc = zapcore.NewConsoleEncoder(conf.EncoderConfig)
	default:
		panic("unknown encoding")
	}
	return zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewCore(enc, ws, conf.Level)
	})
}

func GetWriteSyncer(path string) zapcore.WriteSyncer {
	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		err2 := os.Mkdir(dir, os.ModePerm)
		if err2 != nil {
			panic(err2)
		}
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 644)
	if err != nil {
		panic(err)
	}
	return zapcore.AddSync(file)
}
