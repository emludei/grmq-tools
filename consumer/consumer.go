package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/emludei/grmq-tools/log"
	"github.com/streadway/amqp"
)

// TODO: write detailed comments, test, fix bugs

// Consumer universal consumer with reconnects on broke connection.
type Consumer struct {
	// general fields
	config Config

	logger   log.ILogger
	logLevel log.LogLevel

	// Channel for processing by client.
	pubDeliveryChannel chan amqp.Delivery

	// Channel for invalidation signal. Client has to drop previous unacknowledged messages.
	invalidationChannel chan struct{}

	fatalChannel chan struct{}

	// Mutexes
	// (Mutex)
	reconnectMutex chan struct{}

	// connection fields
	connection      *amqp.Connection
	channel         *amqp.Channel
	deliveryChannel <-chan amqp.Delivery

	// channels for event listeners (connection)
	connCloseChannel chan *amqp.Error

	// channels for event listeners (channel)
	chanCancelChannel chan string

	chanCloseChannel chan *amqp.Error

	// workers sync
	// notify of call Consumer.Close method
	wg sync.WaitGroup

	ctx        context.Context
	cancelFunc context.CancelFunc

	mutex sync.Mutex

	isStarted bool
}

func (c *Consumer) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.isStarted {
		return fmt.Errorf("Consumer.Start() consumer has already started")
	}

	err := c.init()
	if err != nil {
		return fmt.Errorf("Consumer.Start() initialize connection to RabbitMQ error: %v", err)
	}

	c.ctx, c.cancelFunc = context.WithCancel(context.Background())

	c.wg.Add(1)
	go c.listenConnCloseChannel()

	c.wg.Add(1)
	go c.listenChanCancelChannel()

	c.wg.Add(1)
	go c.listenChanCloseChannel()

	c.wg.Add(1)
	go c.consume()

	c.isStarted = true

	if c.logLevel >= log.LogLevelInfo {
		c.logger.Log(log.LogLevelInfo, "Consumer.Start() consumer has successfully started")
	}

	return nil
}

// Close RabbitMQ channel, connection
func (c *Consumer) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.isStarted {
		return fmt.Errorf("Consumer.Stop() consumer has not started")
	}

	if c.cancelFunc == nil {
		return fmt.Errorf("Consumer.Stop() cancel func is nil")
	}

	if c.logLevel >= log.LogLevelInfo {
		c.logger.Log(log.LogLevelInfo, "Consumer.Stop() execute cancel func")
	}

	c.cancelFunc()

	if c.logLevel >= log.LogLevelInfo {
		c.logger.Log(log.LogLevelInfo, "Consumer.Stop() wait for stopping of all goroutines")
	}

	c.wg.Wait()
	c.isStarted = false

	if c.logLevel >= log.LogLevelInfo {
		c.logger.Log(log.LogLevelInfo, "Consumer.Stop() start cleaning of public delivery channel")
	}

	c.cleanPublicDeliveryChannel()

	if c.logLevel >= log.LogLevelInfo {
		c.logger.Log(log.LogLevelInfo, "Consumer.Stop() trigger invalidation signal")
	}

	c.triggerInvalidation()

	if c.logLevel >= log.LogLevelInfo {
		c.logger.Log(log.LogLevelInfo, "Consumer.Stop() close channel and connection to RabbitMQ")
	}

	err := c.channel.Close()
	if err != nil && c.logLevel >= log.LogLevelError {
		msg := fmt.Sprintf("Consumer.Stop() close channel error: %v", err)
		c.logger.Log(log.LogLevelError, msg)
	}

	err = c.connection.Close()
	if err != nil && c.logLevel >= log.LogLevelError {
		msg := fmt.Sprintf("Consumer.Stop() close connection to RabbitMQ error: %v", err)
		c.logger.Log(log.LogLevelError, msg)
	}

	return nil
}

func (c *Consumer) DeliveryChannel() <-chan amqp.Delivery {
	return c.pubDeliveryChannel
}

func (c *Consumer) InvalidationChannel() <-chan struct{} {
	return c.invalidationChannel
}

func (c *Consumer) FatalChannel() <-chan struct{} {
	return c.fatalChannel
}

func (c *Consumer) consume() {
	defer func() {
		if r := recover(); r != nil {
			// TODO: logging

			c.triggerFatal()
		}

		c.wg.Done()
	}()

	done := c.ctx.Done()

	for {
		select {
		case <-done:
			return
		case delivery, ok := <-c.deliveryChannel:
			if !ok {
				go c.reconnect()
				return
			}

			select {
			case <-done:
				return
			case c.pubDeliveryChannel <- delivery:
			}
		case <-time.After(60 * time.Second):
			// TODO: logging, timeout to constant
			go c.reconnect()
			return
		}
	}
}

func (c *Consumer) reconnect() {
	select {
	case c.reconnectMutex <- struct{}{}:
		for {
			time.Sleep(c.config.ReconnectTimeout)

			err := c.Stop()
			if err != nil {
				// TODO: log it
			}

			err = c.Start()
			if err != nil {
				errMessage := fmt.Errorf("Consumer.reconnect() start consuming error: %v", err)
				// TODO: log error

				// try to reconnect again
				continue
			}

			// Connection successfully established.
			// (Mutex) release mutex.
			<-c.reconnectMutex
			return
		}

	default:
		// Some goroutine is already trying to reconnect
		return
	}
}

func (c *Consumer) cleanPublicDeliveryChannel() {
	for len(c.pubDeliveryChannel) > 0 {
		_, ok := <-c.pubDeliveryChannel
		if !ok {
			// TODO log error
			return
		}
	}

	// TODO: log info
}

func (c *Consumer) listenConnCloseChannel() {
	defer c.wg.Done()

	for {
		select {
		case err, ok := <-c.connCloseChannel:
			// this can`t happen, but just in case
			if !ok {
				errString := "Consumer.listenConnCloseChannel() Consumer.connCloseChannel channel closed"
				errMessage := fmt.Errorf(errString)

				// TODO: Log
				go c.reconnect()
				return
			}

			errString := "Consumer.listenConnCloseChannel() connection closed: %v"
			errMessage := fmt.Errorf(errString, err)

			// TODO: log

			go c.reconnect()
			return

		case <-c.ctx.Done():
			// Received exit event, return
			return
		}
	}
}

func (c *Consumer) listenChanCancelChannel() {
	defer c.wg.Done()

	for {
		select {
		case tag, ok := <-c.chanCancelChannel:
			// this can`t happen, but just in case
			if !ok {
				errString := "Consumer.listenChanCancelChannel() Consumer.chanCancelChannel channel closed"
				errMessage := fmt.Errorf(errString)

				// TODO: log

				go c.reconnect()
				return
			}

			errString := "Consumer.listenChanCancelChannel() consume queue canceled: subscription tag %s"
			errMessage := fmt.Errorf(errString, tag)

			// TODO: log

			go c.reconnect()
			return

		case <-c.ctx.Done():
			// Received exit event, return
			return
		}
	}
}

func (c *Consumer) listenChanCloseChannel() {
	defer c.wg.Done()

	for {
		select {
		case err, ok := <-c.chanCloseChannel:
			// this can`t happen, but just in case
			if !ok {
				errString := "Consumer.listenConnCloseChannel() Consumer.chanCloseChannel channel closed"
				errMessage := fmt.Errorf(errString)

				// TODO: log

				go c.reconnect()
				return
			}

			errString := "Consumer.listenConnCloseChannel() channel closed: %v"
			errMessage := fmt.Errorf(errString, err)

			// TODO: log

			go c.reconnect()
			return

		case <-c.ctx.Done():
			// Received exit event, return
			return
		}
	}
}

// Acknowledge delivery message with reconnects
func (c *Consumer) Ack(delivery amqp.Delivery, multiple bool) error {
	if delivery.Acknowledger == nil {
		// if the channel from which this delivery arrived is nil -> skip
		return errors.New("Consumer.Ack() delivery not initialized")
	}

	err := delivery.Acknowledger.Ack(delivery.DeliveryTag, multiple)
	if err != nil {
		go c.reconnect()
		return fmt.Errorf("Consumer.Ack() ack delivery: %v", err)
	}

	return nil
}

// Negatively acknowledge delivery message with reconnects
func (c *Consumer) Nack(delivery amqp.Delivery, multiple, requeue bool) error {
	if delivery.Acknowledger == nil {
		// if the channel from which this delivery arrived is nil -> skip
		return errors.New("Consumer.Nack() delivery not initialized")
	}

	err := delivery.Acknowledger.Nack(delivery.DeliveryTag, multiple, requeue)
	if err != nil {
		go c.reconnect()
		return fmt.Errorf("Consumer.Nack() try nAck delivery: %v", err)
	}

	return nil
}

// Initialize connection to RabbitMQ server, channel.
func (c *Consumer) init() (err error) {
	c.connection, err = amqp.Dial(c.config.URI)
	if err != nil {
		return fmt.Errorf("Consumer.Init() connect to RabbitMQ (%s) error: %v", c.config.URI, err)
	}
	defer func() {
		if err != nil {
			// close established connection in case of error
			closeErr := c.connection.Close()
			if closeErr != nil {
				err = fmt.Errorf("%v: Consumer.Init() close connection to RabbitMQ error: %v", err, closeErr)
			}
		}
	}()

	c.channel, err = c.connection.Channel()
	if err != nil {
		errString := "Consumer.Init() create channel to RabbitMQ (%s) error: %v"
		return fmt.Errorf(errString, c.config.URI, err)
	}
	defer func() {
		if err != nil {
			// close established channel in case of error
			closeErr := c.channel.Close()
			if closeErr != nil {
				err = fmt.Errorf("%v: Consumer.Init() close channel error: %v", err, closeErr)
			}
		}
	}()

	err = c.channel.Qos(c.config.PrefetchCount, c.config.PrefetchSize, c.config.PrefetchGlobal)
	if err != nil {
		return fmt.Errorf("Consumer.Init() declare prefetch policy error: %v", err)
	}

	c.deliveryChannel, err = c.channel.Consume(
		c.config.Queue,     // queue
		c.config.Name,      // consumer
		c.config.AutoAck,   // autoAck
		c.config.Exclusive, // exclusive
		c.config.NoLocal,   // noLocal
		c.config.NoWait,    // noWait
		nil,                // TODO: may be I should add support for args
	)
	if err != nil {
		return fmt.Errorf("Consumer.Init() create delivery channel error: %v", err)
	}

	// channels for event listeners (connection)
	connCloseChannel := make(chan *amqp.Error)
	c.connCloseChannel = c.connection.NotifyClose(connCloseChannel)

	// channels for event listeners (channel)
	chanCancelChannel := make(chan string)
	c.chanCancelChannel = c.channel.NotifyCancel(chanCancelChannel)

	chanCloseChannel := make(chan *amqp.Error)
	c.chanCloseChannel = c.channel.NotifyClose(chanCloseChannel)

	return nil
}

func (c *Consumer) triggerFatal() {
	select {
	case c.fatalChannel <- struct{}{}:
	default:
	}
}

func (c *Consumer) triggerInvalidation() {
	select {
	case c.invalidationChannel <- struct{}{}:
	default:
	}
}

// Creates new RabbitMQ consumer
func NewConsumer(cfg Config) *Consumer {
	consumer := &Consumer{
		config:              cfg,
		pubDeliveryChannel:  make(chan amqp.Delivery, 100), // TODO: buffer size to config
		reconnectMutex:      make(chan struct{}, 1),
		fatalChannel:        make(chan struct{}, 1),
		invalidationChannel: make(chan struct{}, 1),
		wg:                  sync.WaitGroup{},
	}
	return consumer
}
