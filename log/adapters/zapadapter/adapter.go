// Package zapadapter provides a logger that writes to a go.uber.org/zap.Logger.
package zapadapter

import (
	gs "github.com/emludei/graceful-shutdown"
	"go.uber.org/zap"
)

type Logger struct {
	logger *zap.Logger
}

func (l *Logger) Log(level gs.LogLevel, msg string) {
	switch level {
	case gs.LogLevelDebug:
		l.logger.Debug(msg)
	case gs.LogLevelInfo:
		l.logger.Info(msg)
	case gs.LogLevelError:
		l.logger.Error(msg)
	default:
		l.logger.Error(msg)
	}
}

func NewLogger(logger *zap.Logger) *Logger {
	return &Logger{logger: logger.WithOptions(zap.AddCallerSkip(1))}
}
