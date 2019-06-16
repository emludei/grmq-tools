package consumer

import (
	"time"
)

type Config struct {
	// String representation of consumer in consumers section of queue page in rmq ui
	Name                 string
	ReconnectTimeout     time.Duration
	ErrChannelBufferSize int

	// Count of goroutines for process messages from delivery channel
	WorkerPoolSize int

	// Connection config
	URI string

	// Prefetch policy config
	PrefetchCount  int
	PrefetchSize   int
	PrefetchGlobal bool

	// Consume queue config
	Queue     string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
}

func (c Config) Validate() error {
	return nil
}
