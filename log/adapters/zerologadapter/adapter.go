// Package zerologadapter provides a logger that writes to a github.com/rs/zerolog.
package zerologadapter

import (
	gs "github.com/emludei/graceful-shutdown"
	"github.com/rs/zerolog"
)

type Logger struct {
	logger zerolog.Logger
}

func (l *Logger) Log(level gs.LogLevel, msg string) {
	var zlevel zerolog.Level

	switch level {
	case gs.LogLevelDebug:
		zlevel = zerolog.DebugLevel
	case gs.LogLevelInfo:
		zlevel = zerolog.InfoLevel
	case gs.LogLevelError:
		zlevel = zerolog.ErrorLevel
	default:
		zlevel = zerolog.ErrorLevel
	}

	l.logger.WithLevel(zlevel).Msg(msg)
}

func NewLogger(logger zerolog.Logger) *Logger {
	return &Logger{
		logger: logger.With().Str("module", "graceful-shutdown").Logger(),
	}
}
