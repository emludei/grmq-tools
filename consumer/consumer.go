package consumer

import (
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// TODO: write detailed comments, test, fix bugs

// RabbitMQ universal consumer with reconnects on broke connection.
type RMQConsumer struct {
	// general fields
	config       *RMQConsumerConfig
	errorChannel chan error

	// Mutexes
	// (Mutex)
	workerPoolMutex chan struct{}
	// (Mutex)
	reconnectMutex chan struct{}

	// connection fields
	connection      *amqp.Connection
	channel         *amqp.Channel
	deliveryChannel <-chan amqp.Delivery

	// messages must be Ack or Nack here.
	processMessageCallback func(*RMQConsumer, amqp.Delivery)

	// channels for event listeners (connection)
	connCloseChannel     chan *amqp.Error
	exitConnCloseChannel chan struct{}

	// channels for event listeners (channel)
	chanCancelChannel     chan string
	exitChanCancelChannel chan struct{}

	chanCloseChannel     chan *amqp.Error
	exitChanCloseChannel chan struct{}

	// workers sync
	// notify of call Consumer.Close method
	workerWaitGroup *sync.WaitGroup
}

// Initialize connection to RabbitMQ server, channel.
func (c *RMQConsumer) InitConsumer() error {
	connection, err := amqp.Dial(c.config.URI)
	if err != nil {
		errString := "RMQConsumer.InitConnection() connect to RabbitMQ (%s): %v"
		errMessage := fmt.Errorf(errString, c.config.URI, err)
		return errMessage
	}
	c.connection = connection

	channel, err := c.connection.Channel()
	if err != nil {
		// close established connection in case of error
		defer c.connection.Close()
		errString := "RMQConsumer.InitConnection() create channel to RabbitMQ (%s): %v"
		errMessage := fmt.Errorf(errString, c.config.URI, err)
		return errMessage
	}
	c.channel = channel

	err = c.channel.Qos(c.config.PrefetchCount, c.config.PrefetchSize, c.config.PrefetchGlobal)
	if err != nil {
		// close established connection and channel in case of error
		defer c.channel.Close()
		defer c.connection.Close()
		errString := "RMQConsumer.InitConnection() declare prefetch policy: %v"
		errMessage := fmt.Errorf(errString, err)
		return errMessage
	}

	deliveryChannel, err := c.channel.Consume(
		c.config.Queue,     // queue
		c.config.Name,      // consumer
		c.config.AutoAck,   // autoAck
		c.config.Exclusive, // exclusive
		c.config.NoLocal,   // noLocal
		c.config.NoWait,    // noWait
		nil,                // args
	)
	if err != nil {
		defer c.channel.Close()
		defer c.connection.Close()
		errString := "RMQConsumer.InitConnection() create delivery channel: %v"
		errMessage := fmt.Errorf(errString, err)
		return errMessage
	}
	c.deliveryChannel = deliveryChannel

	// channels for event listeners (connection)
	connCloseChannel := make(chan *amqp.Error)
	exitConnCloseChannel := make(chan struct{})
	c.connCloseChannel = c.connection.NotifyClose(connCloseChannel)
	c.exitConnCloseChannel = exitConnCloseChannel

	// channels for event listeners (channel)
	chanCancelChannel := make(chan string)
	exitChanCancelChannel := make(chan struct{})
	c.chanCancelChannel = c.channel.NotifyCancel(chanCancelChannel)
	c.exitChanCancelChannel = exitChanCancelChannel

	chanCloseChannel := make(chan *amqp.Error)
	exitChanCloseChannel := make(chan struct{})
	c.chanCloseChannel = c.channel.NotifyClose(chanCloseChannel)
	c.exitChanCloseChannel = exitChanCloseChannel

	return nil
}

func (c *RMQConsumer) listenConnCloseChannel() {
	c.workerWaitGroup.Add(1)
	defer c.workerWaitGroup.Done()

	for {
		select {
		case err, ok := <-c.connCloseChannel:
			// this can`t happen, but just in case
			if !ok {
				errString := "RMQConsumer.listenConnCloseChannel() RMQConsumer.connCloseChannel channel closed"
				errMessage := fmt.Errorf(errString)
				c.errorChannel <- errMessage
				c.reconnect()

				continue
			}

			errString := "RMQConsumer.listenConnCloseChannel() connection closed: %v"
			errMessage := fmt.Errorf(errString, err)
			c.errorChannel <- errMessage
			c.reconnect()

		case <-c.exitConnCloseChannel:
			// Received exit event, return
			return
		}
	}
}

func (c *RMQConsumer) listenChanCancelChannel() {
	c.workerWaitGroup.Add(1)
	defer c.workerWaitGroup.Done()

	for {
		select {
		case tag, ok := <-c.chanCancelChannel:
			// this can`t happen, but just in case
			if !ok {
				errString := "RMQConsumer.listenChanCancelChannel() RMQConsumer.chanCancelChannel channel closed"
				errMessage := fmt.Errorf(errString)
				c.errorChannel <- errMessage
				c.reconnect()

				continue
			}

			errString := "RMQConsumer.listenChanCancelChannel() consume queue canceled: subscription tag %s"
			errMessage := fmt.Errorf(errString, tag)
			c.errorChannel <- errMessage
			c.reconnect()

		case <-c.exitChanCancelChannel:
			// Received exit event, return
			return
		}
	}
}

func (c *RMQConsumer) listenChanCloseChannel() {
	c.workerWaitGroup.Add(1)
	defer c.workerWaitGroup.Done()

	for {
		select {
		case err, ok := <-c.chanCloseChannel:
			// this can`t happen, but just in case
			if !ok {
				errString := "RMQConsumer.listenConnCloseChannel() RMQConsumer.chanCloseChannel channel closed"
				errMessage := fmt.Errorf(errString)
				c.errorChannel <- errMessage
				c.reconnect()

				continue
			}

			errString := "RMQConsumer.listenConnCloseChannel() channel closed: %v"
			errMessage := fmt.Errorf(errString, err)
			c.errorChannel <- errMessage
			c.reconnect()

		case <-c.exitChanCloseChannel:
			// Received exit event, return
			return
		}
	}
}

func (c *RMQConsumer) Consume() {
	go c.listenConnCloseChannel()
	go c.listenChanCancelChannel()
	go c.listenChanCloseChannel()

	for {
		select {
		case delivery, ok := <-c.deliveryChannel:
			if !ok {
				// ... try reconnect, continue
				c.reconnect()

				continue
			}

			// (Mutex)
			c.workerPoolMutex <- struct{}{}
			go c.processDelivery(delivery)
		}
	}
}

func (c *RMQConsumer) processDelivery(delivery amqp.Delivery) {
	c.workerWaitGroup.Add(1)

	defer func() {
		c.workerWaitGroup.Done()

		// release mutex
		<-c.workerPoolMutex
	}()

	c.processMessageCallback(c, delivery)
}

func (c *RMQConsumer) reconnect() {
	select {
	case c.reconnectMutex <- struct{}{}:
		for {
			time.Sleep(c.config.ReconnectTimeout)

			err := c.InitConsumer()
			if err != nil {
				errString := "RMQConsumer.reconnect() initialize connections: %v"
				errMessage := fmt.Errorf(errString, err)
				c.errorChannel <- errMessage

				// try to reconnect again
				continue
			}

			// Connection successfully established.
			// (Mutex) release mutex.
			<-c.reconnectMutex
			return
		}

	default:
		// Some goroutine already try to reconnect, wait reconnection timeout and return (try pass data,
		// through connection again, may be in other goroutine connection successfully established).
		time.Sleep(c.config.ReconnectTimeout)
		return
	}
}

// Acknowledge delivery message with reconnects
func (c *RMQConsumer) Ack(delivery amqp.Delivery, multiple bool) {
	if delivery.Acknowledger == nil {
		errString := "RMQConsumer.Ack() delivery (%v) not initialized"
		errMessage := fmt.Errorf(errString, delivery)
		c.errorChannel <- errMessage

		// if the channel from which this delivery arrived is nil -> skip
		return
	}

	for {
		err := delivery.Ack(multiple)
		if err != nil {
			errString := "RMQConsumer.Ack() try ack delivery (%v): %v"
			errMessage := fmt.Errorf(errString, delivery, err)
			c.errorChannel <- errMessage
			c.reconnect()

			// try to ack delivery again
			continue
		}

		// delivery successfully acknowledged
		break
	}
}

// Negatively acknowledge delivery message with reconnects
func (c *RMQConsumer) Nack(delivery amqp.Delivery, multiple, requeue bool) {
	if delivery.Acknowledger == nil {
		errString := "RMQConsumer.Nack() delivery (%v) not initialized"
		errMessage := fmt.Errorf(errString, delivery)
		c.errorChannel <- errMessage

		// if the channel from which this delivery arrived is nil -> skip
		return
	}

	for {
		err := delivery.Nack(multiple, requeue)
		if err != nil {
			errString := "RMQConsumer.Nack() try nAck delivery (%v): %v"
			errMessage := fmt.Errorf(errString, delivery, err)
			c.errorChannel <- errMessage
			c.reconnect()

			// try to nAck delivery again
			continue
		}

		// delivery successfully unacknowledged
		break
	}
}

// Returns error channel. All errors rised during consuming will be post to it.
// Developer must write goroutine for logging errors from this channel.
func (c *RMQConsumer) ErrorChannel() chan error {
	return c.errorChannel
}

// Close RabbitMQ channel, connection
func (c *RMQConsumer) Close() {
	c.exitConnCloseChannel <- struct{}{}
	c.exitChanCancelChannel <- struct{}{}
	c.exitChanCloseChannel <- struct{}{}

	c.workerWaitGroup.Wait()

	c.channel.Close()
	c.connection.Close()
}

// Creates new RabbitMQ consumer
func NewRMQConsumer(config *RMQConsumerConfig, processMessageCallback func(*RMQConsumer, amqp.Delivery)) *RMQConsumer {
	// All errors will be post to this channel. Developer must write goroutine for logging errors from this channel.
	errChannel := make(chan error, config.ErrChannelBufferSize)
	// (Mutex)
	workerPoolMutex := make(chan struct{}, config.WorkerPoolSize)
	// (Mutex)
	reconnectMutex := make(chan struct{}, 1)

	workerWaitGroup := sync.WaitGroup{}

	consumer := &RMQConsumer{
		config:                 config,
		errorChannel:           errChannel,
		processMessageCallback: processMessageCallback,
		workerPoolMutex:        workerPoolMutex,
		reconnectMutex:         reconnectMutex,
		workerWaitGroup:        &workerWaitGroup,
	}
	return consumer
}
