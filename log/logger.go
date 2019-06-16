package log

import (
	"fmt"
)

type LogLevel int

func (l LogLevel) String() string {
	switch l {
	case LogLevelNone:
		return "NONE"
	case LogLevelError:
		return "ERROR"
	case LogLevelInfo:
		return "INFO"
	case LogLevelDebug:
		return "DEBUG"
	default:
		return fmt.Sprintf("UNKNOWN [%d]", l)
	}
}

const (
	LogLevelNone LogLevel = iota + 1
	LogLevelError
	LogLevelInfo
	LogLevelDebug
)

// Logger is the interface used to get logging from graceful-shutdown internals.
type ILogger interface {
	// Log a message at the given level.
	Log(level LogLevel, msg string)
}
