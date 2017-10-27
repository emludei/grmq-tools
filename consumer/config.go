package consumer

import (
	"time"
)

type RMQConsumerConfig struct {
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

func NewConsumerConfig(name string, reconnectTimeout time.Duration, errChannelBufferSize, workerPoolSize int,
	uri string, prefetchCount, prefetchSize int, prefetchGlobal bool, queue string, autoAck, exclusive,
	noLocal, noWait bool) *RMQConsumerConfig {

	config := &RMQConsumerConfig{
		Name:                 name,
		ReconnectTimeout:     reconnectTimeout,
		ErrChannelBufferSize: errChannelBufferSize,
		WorkerPoolSize:       workerPoolSize,
		URI:                  uri,
		PrefetchCount:        prefetchCount,
		PrefetchSize:         prefetchSize,
		PrefetchGlobal:       prefetchGlobal,
		Queue:                queue,
		AutoAck:              autoAck,
		Exclusive:            exclusive,
		NoLocal:              noLocal,
		NoWait:               noWait,
	}
	return config
}
