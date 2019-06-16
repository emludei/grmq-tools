// Package logrusadapter provides a logger that writes to a github.com/sirupsen/logrus.Logger.
package logrusadapter

import (
	gs "github.com/emludei/graceful-shutdown"
	"github.com/sirupsen/logrus"
)

type Logger struct {
	logger logrus.FieldLogger
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

func NewLogger(logger logrus.FieldLogger) *Logger {
	return &Logger{logger: logger}
}
