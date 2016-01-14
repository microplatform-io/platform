package platform

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

var (
	internalPublisherCount  = 1
	internalSubscriberCount = 1
)

const PUBLISH_RETRIES = 3
const MAX_WORKERS = 50

type AmqpChannel struct {
	channel           *amqp.Channel
	channelMutex      *sync.Mutex
	connectionManager *AmqpConnectionManager
	name              string
	isActive          bool
	isUpdated         chan bool
}

func NewAmqpChannel(connectionManager *AmqpConnectionManager, name string) *AmqpChannel {
	channel := &AmqpChannel{
		connectionManager: connectionManager,
		name:              name,
		channelMutex:      &sync.Mutex{},
		isActive:          false,
		isUpdated:         make(chan bool),
	}

	channel.resetChannel()
	go channel.keepAlive()

	return channel
}

// A safe bet at closing the channel
func (ch *AmqpChannel) Close() error {
	ch.channelMutex.Lock()
	defer ch.channelMutex.Unlock()

	ch.isActive = false

	return ch.channel.Close()
}

func (ch *AmqpChannel) NotifyClose() chan *amqp.Error {
	return ch.channel.NotifyClose(make(chan *amqp.Error))
}

// If the channel is not active, we will block until we have an active one.
func (ch *AmqpChannel) getChannel() *amqp.Channel {
	if !ch.isActive {
		<-ch.isUpdated
	}

	return ch.channel
}

// The amqpChannel keepAlive handles the persistence of a single channels state.
// If a channel recieves a close signal we will close our existing channel and create a new one.
func (ch *AmqpChannel) keepAlive() error {
	for {
		select {
		case amqpError := <-ch.NotifyClose():
			logger.Printf("[AmqpChannel.keepAlive] %s - channel close signal recieved : %v", ch.name, amqpError)

			logger.Printf("[AmqpChannel.keepAlive] %s - channel is being hard closed.", ch.name)
			ch.Close()
			logger.Printf("[AmqpChannel.keepAlive] %s - channel has being hard closed.", ch.name)

			ch.resetChannel()

			logger.Printf("[AmqpChannel.keepAlive] %s - channel has reconnected.", ch.name)
		}
	}

	return nil
}

func (ch *AmqpChannel) resetChannel() {
	ch.channelMutex.Lock()
	ch.isActive = false
	ch.channelMutex.Unlock()

	for !ch.isActive {
		ch.channelMutex.Lock()

		channel, err := ch.connectionManager.GetChannel()
		if err != nil {
			logger.Println("[AmqpChannel.resetChannel] Failed to get channel : ", err)
			ch.channelMutex.Unlock()
			continue
		}

		ch.channel = channel
		ch.isActive = true
		close(ch.isUpdated)
		ch.isUpdated = make(chan bool)
		ch.channelMutex.Unlock()
	}
}

type AmqpPublisher struct {
	amqpChannel *AmqpChannel
}

func (p *AmqpPublisher) Publish(topic string, body []byte) error {
	logger.Printf("[AmqpPublisher.Publish] Publishing for %s", topic)

	var publishErr error
	for i := 0; i < PUBLISH_RETRIES; i++ {
		publishErr = p.amqpChannel.channel.Publish(
			"amq.topic", // exchange
			topic,       // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        body,
			},
		)
		if publishErr == nil {
			logger.Printf("[AmqpPublisher.Publish] published for %s", topic)
			return nil
		}

		logger.Printf("[AmqpPublisher.Publish] Error publishing for %s - %s", topic, publishErr.Error())
		//if we have to reset the channnel, its because of an error so lets always get a new channel

		p.amqpChannel.resetChannel()
	}

	return publishErr
}

func NewAmqpPublisher(connectionManager *AmqpConnectionManager) (*AmqpPublisher, error) {
	publisher := &AmqpPublisher{
		amqpChannel: NewAmqpChannel(connectionManager, fmt.Sprintf("publisher#%d", internalPublisherCount)),
	}

	internalPublisherCount = internalPublisherCount + 1

	return publisher, nil
}

type subscription struct {
	Topic   string
	Handler ConsumerHandler
	msgChan chan amqp.Delivery
}

func (s *subscription) canHandle(msg amqp.Delivery) bool {
	if s.Topic == "" {
		return true
	}

	return s.Topic == msg.RoutingKey
}

func (s *subscription) runWorker() {
	for msg := range s.msgChan {
		incrementConsumedWorkCount()

		s.Handler.HandleMessage(msg.Body)
		msg.Ack(false)

		decrementConsumedWorkCount()
	}
}

func newSubscription(topic string, handler ConsumerHandler) *subscription {
	s := &subscription{
		Topic:   topic,
		Handler: handler,
		msgChan: make(chan amqp.Delivery),
	}

	logger.Printf("[newSubscription] creating '%s' subscription worker pool", topic)

	// TODO: Determine an ideal worker pool
	for i := 0; i <= MAX_WORKERS; i++ {
		go s.runWorker()
	}

	return s
}

type AmqpSubscriber struct {
	queue         string
	subscriptions []*subscription
	exclusive     bool
	autoDelete    bool
	amqpChannel   *AmqpChannel
}

func (s *AmqpSubscriber) Run() error {
	logger.Printf("[AmqpSubscriber.Run] initiating")

	for {
		logger.Println("[AmqpSubscriber.Run] Attempting to run subscription.")
		if err := s.run(); err != nil {
			logger.Printf("[AmqpSubscriber.Run] failed to run subscription: %s", err)
		}
	}

	return nil
}

// Our running of the actual subscriber, where we declare and bind based on the notion of the
// subscriber and handles the messages. If we recieve a signal that the channel is closed
// we will break out and wait for a new connection.
func (s *AmqpSubscriber) run() error {
	channel := s.amqpChannel.getChannel()
	durable := true
	autoDelete := false

	if s.exclusive {
		durable = false
	}

	if s.autoDelete {
		autoDelete = true
	}

	if _, err := channel.QueueDeclare(s.queue, durable, autoDelete, s.exclusive, true, nil); err != nil {
		return err
	}

	for _, subscription := range s.subscriptions {
		logger.Println("> binding", s.queue, "to", subscription.Topic)
		if err := channel.QueueBind(s.queue, subscription.Topic, "amq.topic", true, nil); err != nil {
			return err
		}
	}

	logger.Println("[AmqpSubscriber.run] After finished binding")

	msgs, err := channel.Consume(
		s.queue,     // queue
		"",          // consumer defined by server
		false,       // auto-ack
		s.exclusive, // exclusive
		false,       // no-local
		true,        // no-wait
		nil,         // args
	)
	if err != nil {
		return err
	}

	iterate := true

	closeChan := s.amqpChannel.NotifyClose()

	for iterate {
		select {
		case msg := <-msgs:
			if !stillConsuming && !strings.Contains(msg.RoutingKey, "router") {
				logger.Println("Rejecting the message, because we are shutting down.")
				msg.Reject(true)
				continue
			}

			handled := false

			for _, subscription := range s.subscriptions {
				if subscription.canHandle(msg) {
					select {
					case subscription.msgChan <- msg:
						handled = true

					case <-time.After(500 * time.Millisecond):
						// ignore
					}
				}
			}

			if !handled {
				msg.Reject(true)
			}

		case amqpError := <-closeChan:
			logger.Println("[AmqpSubscriber.run] An event occurred causing the need for a new channel : ", amqpError)
			iterate = false
		}
	}

	return errors.New("connection has been closed")
}

func (s *AmqpSubscriber) Subscribe(topic string, handler ConsumerHandler) {
	s.subscriptions = append(s.subscriptions, newSubscription(topic, handler))
}

func NewAmqpSubscriber(connectionManager *AmqpConnectionManager, queue string) (*AmqpSubscriber, error) {
	subscriber := &AmqpSubscriber{
		queue:       queue,
		exclusive:   false,
		autoDelete:  false,
		amqpChannel: NewAmqpChannel(connectionManager, fmt.Sprintf("subscriber#%d", internalSubscriberCount)),
	}
	internalSubscriberCount = internalSubscriberCount + 1
	return subscriber, nil
}

func NewExclusiveAmqpSubscriber(connectionManager *AmqpConnectionManager, queue string) (*AmqpSubscriber, error) {
	subscriber := &AmqpSubscriber{
		queue:       queue,
		exclusive:   true,
		autoDelete:  false,
		amqpChannel: NewAmqpChannel(connectionManager, fmt.Sprintf("subscriber#%d", internalSubscriberCount)),
	}
	internalSubscriberCount = internalSubscriberCount + 1
	return subscriber, nil
}

func NewAutoDeleteAmqpSubscriber(connectionManager *AmqpConnectionManager, queue string) (*AmqpSubscriber, error) {
	subscriber := &AmqpSubscriber{
		queue:       queue,
		exclusive:   false,
		autoDelete:  true,
		amqpChannel: NewAmqpChannel(connectionManager, fmt.Sprintf("subscriber#%d", internalSubscriberCount)),
	}
	internalSubscriberCount = internalSubscriberCount + 1
	return subscriber, nil
}

// The amqp connection manager handles the persistence of a single connection to be
// reused across multiple clients. If a connection needs to be reset, we will add
// a helper function that resets the connection so that the subsequent connection
// lookup will obtain a new connection.
type AmqpConnectionManager struct {
	endpoint string

	connection       *amqp.Connection
	connectionMutex  *sync.Mutex
	isConnected      bool
	isReconnecting   bool
	connectionUpdate chan bool
}

// Return the existing connection if one has already been established, or
// else we should generate a new connection and cache it for reuse.
func (cm *AmqpConnectionManager) GetConnection(block bool) (*amqp.Connection, error) {
	if cm.isConnected {
		return cm.connection, nil
	}

	if cm.isReconnecting {
		if block {
			<-cm.connectionUpdate
			return cm.GetConnection(block)
		} else {
			return nil, errors.New("connection is being reconnected")
		}
	}

	return nil, errors.New("connection is permanently disconnected")
}

// Returns a new channel from the current connection. We will block until we get a connection
// and should bubble up the results of trying to obtain the channel.
func (cm *AmqpConnectionManager) GetChannel() (*amqp.Channel, error) {
	connection, err := cm.GetConnection(true)
	if err != nil {
		logger.Println("[AmqpConnectionManager.GetChannel] Failed to get connection : ", err)
		return nil, err
	}

	return connection.Channel()
}

func (cm *AmqpConnectionManager) keepAlive() {
	for i := 0; i < 50; i++ {
		logger.Printf("> attempting to connect: %#v", cm)

		cm.connectionMutex.Lock()
		connection, err := amqp.Dial(cm.endpoint)
		if err != nil {
			logger.Println("> failed to connect:", err)
			time.Sleep(time.Duration((i%5)+1) * time.Second)
			cm.connectionMutex.Unlock()
			continue
		}

		logger.Println("[AmqpConnectionManager.ConnectionLoop] We are now connected.")

		cm.connection = connection
		cm.isConnected = true
		cm.isReconnecting = false
		close(cm.connectionUpdate)

		cm.connectionMutex.Unlock()

		<-connection.NotifyClose(make(chan *amqp.Error, 0))
		logger.Println("[AmqpConnectionManager.ConnectionLoop] Connection has been broken, attempting to reconnect")

		// Reset i to attempt to reconnect 50 times again
		i = 0
		cm.connectionMutex.Lock()
		cm.connection = nil
		cm.isConnected = false
		cm.isReconnecting = true
		cm.connectionUpdate = make(chan bool)
		cm.connectionMutex.Unlock()
	}

	// 50 attempts for a single connection have failed, mark as permanent failure
	cm.isConnected = false
	cm.isReconnecting = false
}

// Generate a new connection manager with all of the credentials needed to establish
// an AMQP connection. Addr comes in the form of host:port, if no port is provided
// then the standard port 5672 will be used
func NewAmqpConnectionManager(user, pass, addr, virtualHost string) *AmqpConnectionManager {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
		port = "5672"
	}

	amqpConnectionManager := &AmqpConnectionManager{
		endpoint:         "amqp://" + user + ":" + pass + "@" + host + ":" + port + "/" + virtualHost,
		connection:       nil,
		connectionMutex:  &sync.Mutex{},
		isConnected:      false,
		isReconnecting:   true,
		connectionUpdate: make(chan bool),
	}

	go amqpConnectionManager.keepAlive()

	return amqpConnectionManager
}

func NewAmqpConnectionManagerWithEndpoint(endpoint string) *AmqpConnectionManager {
	amqpConnectionManager := &AmqpConnectionManager{
		endpoint:         endpoint,
		connection:       nil,
		connectionMutex:  &sync.Mutex{},
		isConnected:      false,
		isReconnecting:   true,
		connectionUpdate: make(chan bool),
	}

	go amqpConnectionManager.keepAlive()

	return amqpConnectionManager
}
