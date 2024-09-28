package log

import (
	"go.uber.org/zap/zapcore"
	"runtime"
)

const callerSkipOffset = 2

type levelFilterCore struct {
	core        zapcore.Core
	level       zapcore.LevelEnabler
	skip        int
	levelCaller map[zapcore.Level]interface{}
}

// NewLogTraceLevelCore creates a core that can be used to increase the level of
// an existing Core. It cannot be used to decrease the logging level, as it acts
// as a filter before calling the underlying core. If level decreases the log level,
// an error is returned.
func NewLogTraceLevelCore(core zapcore.Core, level zapcore.LevelEnabler, skip int, levelCaller ...zapcore.Level) (zapcore.Core, error) {
	lc := make(map[zapcore.Level]interface{}, 0)
	if len(levelCaller) > 0 {
		for _, z := range levelCaller {
			lc[z] = z
		}
	}
	return &levelFilterCore{core, level, skip, lc}, nil
}

func (c *levelFilterCore) Enabled(lvl zapcore.Level) bool {
	return c.level.Enabled(lvl)
}

func (c *levelFilterCore) With(fields []zapcore.Field) zapcore.Core {
	return &levelFilterCore{c.core.With(fields), c.level, c.skip, c.levelCaller}
}

func (c *levelFilterCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if _, ok := c.levelCaller[ent.Level]; ok {
		frame, defined := getCallerFrame(c.skip + callerSkipOffset)
		ent.Caller = zapcore.EntryCaller{
			Defined:  defined,
			PC:       frame.PC,
			File:     frame.File,
			Line:     frame.Line,
			Function: frame.Function,
		}
		return ce.AddCore(ent, c.core)
	}
	if !c.core.Enabled(ent.Level) {
		return ce
	}
	return c.core.Check(ent, ce)
}

func (c *levelFilterCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	return c.core.Write(ent, fields)
}

func (c *levelFilterCore) Sync() error {
	return c.core.Sync()
}

func getCallerFrame(skip int) (frame runtime.Frame, ok bool) {
	const skipOffset = 2 // skip getCallerFrame and Callers

	pc := make([]uintptr, 1)
	numFrames := runtime.Callers(skip+skipOffset, pc)
	if numFrames < 1 {
		return
	}

	frame, _ = runtime.CallersFrames(pc).Next()
	return frame, frame.PC != 0
}
