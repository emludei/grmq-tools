package consumer

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/emludei/grmq-tools/log"
	"github.com/streadway/amqp"
)

// TODO: write detailed comments, test, fix bugs

const (
	consumerIdleTimeout = 60 * time.Second
)

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

	if c.logLevel >= log.LogLevelDebug {
		c.logger.Log(log.LogLevelDebug, "Consumer.Start() start listening of connection close events")
	}

	c.wg.Add(1)
	go c.listenConnCloseChannel()

	if c.logLevel >= log.LogLevelDebug {
		c.logger.Log(log.LogLevelDebug, "Consumer.Start() start listening of channel cancel events")
	}

	c.wg.Add(1)
	go c.listenChanCancelChannel()

	if c.logLevel >= log.LogLevelDebug {
		c.logger.Log(log.LogLevelDebug, "Consumer.Start() start listening of channel close events")
	}

	c.wg.Add(1)
	go c.listenChanCloseChannel()

	if c.logLevel >= log.LogLevelDebug {
		c.logger.Log(log.LogLevelDebug, "Consumer.Start() start consuming")
	}

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

	if c.logLevel >= log.LogLevelDebug {
		c.logger.Log(log.LogLevelDebug, "Consumer.Stop() execute cancel func")
	}

	c.cancelFunc()

	if c.logLevel >= log.LogLevelDebug {
		c.logger.Log(log.LogLevelDebug, "Consumer.Stop() wait for stopping of all goroutines")
	}

	c.wg.Wait()
	c.isStarted = false

	if c.logLevel >= log.LogLevelDebug {
		c.logger.Log(log.LogLevelDebug, "Consumer.Stop() start cleaning of public delivery channel")
	}

	c.cleanPublicDeliveryChannel()

	if c.logLevel >= log.LogLevelDebug {
		c.logger.Log(log.LogLevelDebug, "Consumer.Stop() trigger invalidation signal")
	}

	c.triggerInvalidation()

	if c.logLevel >= log.LogLevelDebug {
		c.logger.Log(log.LogLevelDebug, "Consumer.Stop() close channel and connection to RabbitMQ")
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

	if c.logLevel >= log.LogLevelInfo {
		c.logger.Log(log.LogLevelInfo, "Consumer.Stop() consumer has successfully stopped")
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
			if c.logLevel >= log.LogLevelError {
				msg := fmt.Sprintf("Consumer.consume() panic: (%v), at: [[\n%s\n]]\n", r, debug.Stack())
				c.logger.Log(log.LogLevelError, msg)
			}

			c.triggerFatal()
		}

		c.wg.Done()
	}()

	if c.logLevel >= log.LogLevelDebug {
		c.logger.Log(log.LogLevelDebug, "Consumer.consume() consuming has started")
	}

	done := c.ctx.Done()

	for {
		select {
		case <-done:
			if c.logLevel >= log.LogLevelDebug {
				c.logger.Log(log.LogLevelDebug, "Consumer.consume() cancel consuming via context")
			}
			return
		case delivery, ok := <-c.deliveryChannel:
			if !ok {
				if c.logLevel >= log.LogLevelError {
					msg := "Consumer.consume() delivery channel was unexpectedly closed, start reconnecting"
					c.logger.Log(log.LogLevelError, msg)
				}

				go c.reconnect()
				return
			}

			select {
			case <-done:
				if c.logLevel >= log.LogLevelDebug {
					c.logger.Log(log.LogLevelDebug, "Consumer.consume() cancel consuming via context")
				}
				return
			case c.pubDeliveryChannel <- delivery:
				if c.logLevel >= log.LogLevelDebug {
					c.logger.Log(log.LogLevelDebug, "Consumer.consume() publish delivery to public channel")
				}
			}
		case <-time.After(consumerIdleTimeout):
			if c.logLevel >= log.LogLevelInfo {
				msg := "Consumer.consume() during %s has not processed any messages, start reconnecting"
				c.logger.Log(log.LogLevelInfo, msg)
			}

			go c.reconnect()
			return
		}
	}
}

func (c *Consumer) reconnect() {
	select {
	case c.reconnectMutex <- struct{}{}:
		if c.logLevel >= log.LogLevelDebug {
			c.logger.Log(log.LogLevelDebug, "Consumer.reconnect() reconnecting has started")
		}

		for {
			time.Sleep(c.config.ReconnectTimeout)

			if c.logLevel >= log.LogLevelDebug {
				c.logger.Log(log.LogLevelDebug, "Consumer.reconnect() try to reconnect")
			}

			err := c.Stop()
			if err != nil && c.logLevel >= log.LogLevelError {
				msg := fmt.Sprintf("Consumer.reconnect() stop consuming error: %v", err)
				c.logger.Log(log.LogLevelError, msg)
			}

			err = c.Start()
			if err != nil {
				if c.logLevel >= log.LogLevelError {
					msg := fmt.Sprintf("Consumer.reconnect() start consuming error: %v", err)
					c.logger.Log(log.LogLevelError, msg)
				}

				// try to reconnect again
				continue
			}

			if c.logLevel >= log.LogLevelDebug {
				c.logger.Log(log.LogLevelDebug, "Consumer.reconnect() connection has successfully established")
			}

			// Connection successfully established.
			// (Mutex) release mutex.
			<-c.reconnectMutex
			return
		}

	default:
		// Some goroutine is already trying to reconnect
		if c.logLevel >= log.LogLevelDebug {
			c.logger.Log(log.LogLevelDebug, "Consumer.reconnect() some goroutine is already trying to reconnect")
		}
		return
	}
}

func (c *Consumer) cleanPublicDeliveryChannel() {
	for len(c.pubDeliveryChannel) > 0 {
		_, ok := <-c.pubDeliveryChannel
		if !ok {
			if c.logLevel >= log.LogLevelError {
				msg := "Consumer.cleanPublicDeliveryChannel() public delivery channel has unexpectedly closed"
				c.logger.Log(log.LogLevelError, msg)
			}

			return
		}
	}

	if c.logLevel >= log.LogLevelDebug {
		msg := "Consumer.cleanPublicDeliveryChannel() public delivery channel has cleaned"
		c.logger.Log(log.LogLevelDebug, msg)
	}
}

func (c *Consumer) listenConnCloseChannel() {
	defer c.wg.Done()

	for {
		select {
		case err, ok := <-c.connCloseChannel:
			if !ok {
				if c.logLevel >= log.LogLevelError {
					msg := "Consumer.listenConnCloseChannel() Consumer.connCloseChannel " +
						"channel has closed unexpectedly, start restarting"
					c.logger.Log(log.LogLevelError, msg)
				}

				go c.reconnect()
				return
			}

			if c.logLevel >= log.LogLevelError {
				msg := fmt.Sprintf("Consumer.listenConnCloseChannel() connection closed error: %v", err)
				c.logger.Log(log.LogLevelError, msg)
			}

			go c.reconnect()
			return

		case <-c.ctx.Done():
			if c.logLevel >= log.LogLevelDebug {
				c.logger.Log(log.LogLevelDebug, "Consumer.listenConnCloseChannel() cancel via context")
			}

			return
		}
	}
}

func (c *Consumer) listenChanCancelChannel() {
	defer c.wg.Done()

	for {
		select {
		case tag, ok := <-c.chanCancelChannel:
			if !ok {
				if c.logLevel >= log.LogLevelError {
					msg := "Consumer.listenChanCancelChannel() Consumer.chanCancelChannel " +
						"channel has closed unexpectedly, start restarting"
					c.logger.Log(log.LogLevelError, msg)
				}

				go c.reconnect()
				return
			}

			if c.logLevel >= log.LogLevelError {
				msg := fmt.Sprintf(
					"Consumer.listenChanCancelChannel() consuming has canceled: subscription tag %s",
					tag)
				c.logger.Log(log.LogLevelError, msg)
			}

			go c.reconnect()
			return

		case <-c.ctx.Done():
			if c.logLevel >= log.LogLevelDebug {
				c.logger.Log(log.LogLevelDebug, "Consumer.listenChanCancelChannel() cancel via context")
			}

			return
		}
	}
}

func (c *Consumer) listenChanCloseChannel() {
	defer c.wg.Done()

	for {
		select {
		case err, ok := <-c.chanCloseChannel:
			if !ok {
				if c.logLevel >= log.LogLevelError {
					msg := "Consumer.listenChanCloseChannel() Consumer.chanCloseChannel " +
						"channel has closed unexpectedly, start restarting"
					c.logger.Log(log.LogLevelError, msg)
				}

				go c.reconnect()
				return
			}

			if c.logLevel >= log.LogLevelError {
				msg := fmt.Sprintf("Consumer.listenConnCloseChannel() channel closed error: %v", err)
				c.logger.Log(log.LogLevelError, msg)
			}

			go c.reconnect()
			return

		case <-c.ctx.Done():
			if c.logLevel >= log.LogLevelDebug {
				c.logger.Log(log.LogLevelDebug, "Consumer.listenConnCloseChannel() cancel via context")
			}

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
