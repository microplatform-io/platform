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
const MAX_WORKERS = 20

type AmqpChannel struct {
	channel           *amqp.Channel
	channelMutex      *sync.Mutex
	connectionManager *AmqpConnectionManager
	name              string
	isActive          bool
	closeChan         chan *amqp.Error
}

func NewAmqpChannel(connectionManager *AmqpConnectionManager, name string) *AmqpChannel {
	return &AmqpChannel{
		connectionManager: connectionManager,
		name:              name,
		channelMutex:      &sync.Mutex{},
		closeChan:         make(chan *amqp.Error),
		isActive:          false,
	}
}

// The amqpChannel keepAlive handles the persistence of a single channels state.
// If a connection needs to be reset, we will wait for the connection to reconnect and settle
// and then we will recreate a channel, if a channel recieves a close signal we will close our existing
// channel and create a new one.
func (ch *AmqpChannel) keepAlive() error {
	connClosed := ch.connectionManager.connection.NotifyClose(make(chan *amqp.Error))
	chanClosed := ch.channel.NotifyClose(make(chan *amqp.Error))

	for {

		select {
		case amqpError := <-connClosed:
			ch.channelMutex.Lock()
			ch.isActive = false
			ch.channelMutex.Unlock()

			logger.Printf("[AmqpChannel.keepAlive] %s - channel connection close signal recieved : %v", ch.name, amqpError)

			//Wait a bit so we dont have to close the connection ourself. We also do this so we don't try to close
			//the connection right after its been reconnected, or try to create a channel with a stale connection.
			//We let the connection routine do its job
			time.Sleep(50 * time.Millisecond)

			//reset channel handles waiting for the connection now
			if err := ch.resetChannel(); err != nil {
				logger.Println("[AmqpChannel.keepAlive] Failed to reset channel after connection reset : ", err)
				ch.closeChan <- amqpError
				return err
			}

			logger.Printf("[AmqpChannel.keepAlive] %s - channel connection has reconnected", ch.name)

		case amqpError := <-chanClosed:
			ch.channelMutex.Lock()
			ch.isActive = false
			ch.channelMutex.Unlock()

			logger.Printf("[AmqpChannel.keepAlive] %s - channel close signal recieved : %v", ch.name, amqpError)

			if ch.channel != nil {
				logger.Printf("[AmqpChannel.keepAlive] %s - channel is being hard closed.", ch.name)
				ch.channel.Close()
				logger.Printf("[AmqpChannel.keepAlive] %s - channel has being hard closed.", ch.name)
			}

			if err := ch.resetChannel(); err != nil {
				logger.Println("[AmqpChannel.keepAlive] Failed to reset channel : ", err)
				return err
			}

			logger.Printf("[AmqpChannel.keepAlive] %s - channel has reconnected.", ch.name)

		default:
			continue
		}

		connClosed = ch.connectionManager.connection.NotifyClose(make(chan *amqp.Error))
		chanClosed = ch.channel.NotifyClose(make(chan *amqp.Error))
	}

	return nil
}

func (amqpChannel *AmqpChannel) resetChannel() error {
	if amqpChannel.connectionManager.connection == nil || !amqpChannel.connectionManager.isConnected || !amqpChannel.connectionManager.isReconnecting {
		logger.Printf("[AmqpChannel.resetChannel] updating connection")

		connection, err := amqpChannel.connectionManager.GetConnection(true)
		if err != nil {
			logger.Println("[AmqpChannel.resetChannel] Failed to get connection : ", err)
			return err
		}
		amqpChannel.connectionManager.connectionMutex.Lock()
		amqpChannel.connectionManager.connection = connection
		amqpChannel.connectionManager.connectionMutex.Unlock()
	}

	channel, err := amqpChannel.connectionManager.connection.Channel()
	if err != nil {
		logger.Println("[AmqpChannel.resetChannel] Failed to get channel : ", err)
		return err
	}

	amqpChannel.channelMutex.Lock()
	amqpChannel.channel = channel
	amqpChannel.isActive = true
	amqpChannel.closeChan = make(chan *amqp.Error)
	amqpChannel.channelMutex.Unlock()
	return nil
}

type AmqpPublisher struct {
	amqpChannel *AmqpChannel
}

func (p *AmqpPublisher) Publish(topic string, body []byte) error {
	if p.amqpChannel.channel == nil {
		logger.Printf("CHANNEL IS :", p.amqpChannel.channel)
		if err := p.amqpChannel.resetChannel(); err != nil {
			return err
		}
	}

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

	//this will get us a channel before we start listening
	if err := publisher.amqpChannel.resetChannel(); err != nil {
		return nil, err
	}

	go publisher.amqpChannel.keepAlive()

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
	for i := 0; i <= 20; i++ {
		go s.runWorker()
	}

	return s
}

type AmqpSubscriber struct {
	queue         string
	subscriptions []*subscription
	exclusive     bool
	amqpChannel   *AmqpChannel
}

func (s *AmqpSubscriber) Run() error {
	logger.Printf("[AmqpSubscriber.Run] initiating")

	for {
		_, err := s.amqpChannel.connectionManager.GetConnection(true)
		if err != nil {
			logger.Printf("[AmqpSubscriber.Run] failed to get connection: %s", err)
			continue
		}

		if !s.amqpChannel.isActive {
			continue
		}

		logger.Println("[AmqpSubscriber.Run] Attempting to run subscription.")
		if err := s.run(); err != nil {
			logger.Printf("[AmqpSubscriber.Run] failed to run subscription: %s", err)
		}
	}

	return nil
}

func (s *AmqpSubscriber) run() error {
	durable := true
	autoDelete := false

	if s.exclusive {
		durable = false
		autoDelete = true
	}

	if _, err := s.amqpChannel.channel.QueueDeclare(s.queue, durable, autoDelete, s.exclusive, true, nil); err != nil {
		return err
	}

	for _, subscription := range s.subscriptions {
		logger.Println("> binding", s.queue, "to", subscription.Topic)
		if err := s.amqpChannel.channel.QueueBind(s.queue, subscription.Topic, "amq.topic", true, nil); err != nil {
			return err
		}
	}

	logger.Println("[AmqpSubscriber.run] After finished binding")

	msgs, err := s.amqpChannel.channel.Consume(
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

		case closeChan := <-s.amqpChannel.closeChan:
			logger.Println("[AmqpSubscriber.run] An event occurred causing the need for a new channel : ", closeChan)
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
		amqpChannel: NewAmqpChannel(connectionManager, fmt.Sprintf("subscriber#%d", internalSubscriberCount)),
	}

	internalSubscriberCount = internalSubscriberCount + 1

	err := subscriber.amqpChannel.resetChannel()
	if err != nil {
		return nil, err
	}

	go subscriber.amqpChannel.keepAlive()

	return subscriber, nil
}

func NewExclusiveAmqpSubscriber(connectionManager *AmqpConnectionManager, queue string) (*AmqpSubscriber, error) {
	subscriber := &AmqpSubscriber{
		queue:       queue,
		exclusive:   true,
		amqpChannel: NewAmqpChannel(connectionManager, fmt.Sprintf("subscriber#%d", internalSubscriberCount)),
	}

	internalSubscriberCount = internalSubscriberCount + 1

	err := subscriber.amqpChannel.resetChannel()
	if err != nil {
		return nil, err
	}

	go subscriber.amqpChannel.keepAlive()

	return subscriber, nil
}

// The amqp connection manager handles the persistence of a single connection to be
// reused across multiple clients. If a connection needs to be reset, we will add
// a helper function that resets the connection so that the subsequent connection
// lookup will obtain a new connection.
type AmqpConnectionManager struct {
	user        string
	pass        string
	host        string
	port        string
	virtualHost string

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
		user:        user,
		pass:        pass,
		host:        host,
		port:        port,
		virtualHost: virtualHost,

		connection:       nil,
		connectionMutex:  &sync.Mutex{},
		isConnected:      false,
		isReconnecting:   true,
		connectionUpdate: make(chan bool),
	}

	go func() {
		for i := 0; i < 50; i++ {
			logger.Printf("> attempting to connect: %#v", amqpConnectionManager)

			amqpConnectionManager.connectionMutex.Lock()
			connection, err := amqp.Dial("amqp://" + amqpConnectionManager.user + ":" + amqpConnectionManager.pass + "@" + amqpConnectionManager.host + ":" + amqpConnectionManager.port + "/" + amqpConnectionManager.virtualHost)
			if err != nil {
				logger.Println("> failed to connect:", err)
				time.Sleep(time.Duration((i%5)+1) * time.Second)
				continue
			}

			logger.Println("[AmqpConnectionManager.ConnectionLoop] We are now connected.")

			amqpConnectionManager.connection = connection
			amqpConnectionManager.isConnected = true
			amqpConnectionManager.isReconnecting = false
			close(amqpConnectionManager.connectionUpdate)

			amqpConnectionManager.connectionMutex.Unlock()

			<-connection.NotifyClose(make(chan *amqp.Error, 0))
			logger.Println("[AmqpConnectionManager.ConnectionLoop] Connection has been broken, attempting to reconnect")

			// Reset i to attempt to reconnect 50 times again
			i = 0
			amqpConnectionManager.connectionMutex.Lock()
			amqpConnectionManager.connection = nil
			amqpConnectionManager.isConnected = false
			amqpConnectionManager.isReconnecting = true
			amqpConnectionManager.connectionUpdate = make(chan bool)
			amqpConnectionManager.connectionMutex.Unlock()
		}

		// 50 attempts for a single connection have failed, mark as permanent failure
		amqpConnectionManager.isConnected = false
		amqpConnectionManager.isReconnecting = false
	}()

	return amqpConnectionManager
}

//this should shoot off our reconnecting logic when the connection recieves the close signal
func (cm *AmqpConnectionManager) CloseConnection() {
	cm.connectionMutex.Lock()
	defer cm.connectionMutex.Unlock()

	//if we are already in bad state no need to move foward
	if cm.connection == nil || !cm.isConnected || cm.isReconnecting {
		return
	}

	logger.Println("[AmqpConnectionManager.CloseConnection] We are attempting to manually close the connection.")

	//by saying that its no longer connected, we just just always fetch a new connection
	cm.isConnected = false
	cm.isReconnecting = true
	logger.Println("[AmqpConnectionManager.CloseConnection] We have manually closed the connection.")

}
