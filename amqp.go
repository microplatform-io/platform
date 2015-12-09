package platform

import (
	"errors"
	"net"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

const PUBLISH_RETRIES = 3
const MAX_WORKERS = 20

type AmqpPublisher struct {
	connectionManager *AmqpConnectionManager
	channel           *amqp.Channel
}

func (p *AmqpPublisher) Publish(topic string, body []byte) error {
	if p.channel == nil {
		logger.Printf("CHANNEL IS :", p.channel)
		if err := p.resetChannel(); err != nil {
			return err
		}
	}

	logger.Printf("[amqp] > publishing for %s", topic)

	var publishErr error

	for i := 0; i < PUBLISH_RETRIES; i++ {
		publishErr = p.channel.Publish(
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
			logger.Printf("[amqp] > published for %s", topic)
			return nil
		}

		logger.Printf("[amqp] > Error publishing for %s - %s", topic, publishErr.Error())
		//if we have to reset the channnel, its because of an error so lets always get a new channel
		//by having connection.close() we should immediently try to reconnect
		p.connectionManager.CloseConnection()
		p.resetChannel()
	}

	return publishErr
}

func (p *AmqpPublisher) resetChannel() error {
	connection, err := p.connectionManager.GetConnection(true)
	if err != nil {
		return err
	}

	channel, err := connection.Channel()
	if err != nil {
		return err
	}

	p.channel = channel

	return nil
}

func NewAmqpPublisher(connectionManager *AmqpConnectionManager) (*AmqpPublisher, error) {
	return &AmqpPublisher{connectionManager: connectionManager}, nil
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
	connectionManager *AmqpConnectionManager
	queue             string
	subscriptions     []*subscription
	exclusive         bool
}

func (s *AmqpSubscriber) Run() error {
	logger.Printf("[AmqpSubscriber.run] initiating")

	for {
		if err := s.run(); err != nil {
			logger.Printf("[AmqpSubscriber.run] failed to run subscription: %s", err)
		}
		logger.Println("[AmqpSubscriber.run] Attempting to run again.")
	}

	return nil
}

func (s *AmqpSubscriber) run() error {
	logger.Printf("> AmqpSubscriber.run: grabbing connection")

	conn, err := s.connectionManager.GetConnection(true)
	if err != nil {
		return err
	}

	logger.Printf("> AmqpSubscriber.run: got connection: %#v", conn)

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	logger.Printf("> AmqpSubscriber.run: got channel")

	durable := true
	autoDelete := false

	if s.exclusive {
		durable = false
		autoDelete = true
	}

	if _, err := ch.QueueDeclare(s.queue, durable, autoDelete, s.exclusive, false, nil); err != nil {
		return err
	}

	for _, subscription := range s.subscriptions {
		logger.Println("> binding", s.queue, "to", subscription.Topic)
		if err := ch.QueueBind(s.queue, subscription.Topic, "amq.topic", false, nil); err != nil {
			return err
		}
	}

	msgs, err := ch.Consume(
		s.queue,     // queue
		"",          // consumer defined by server
		false,       // auto-ack
		s.exclusive, // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return err
	}

	iterate := true

	connClosed := conn.NotifyClose(make(chan *amqp.Error))
	chanClosed := ch.NotifyClose(make(chan *amqp.Error))

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

		case <-connClosed:
			logger.Println("[AmqpSubscriber.run] connection has been closed")

			iterate = false

		case <-chanClosed:
			logger.Println("[AmqpSubscriber.run] channel has been closed")

			iterate = false
		}
	}

	s.connectionManager.CloseConnection()
	return errors.New("connection has been closed")
}

func (s *AmqpSubscriber) Subscribe(topic string, handler ConsumerHandler) {
	s.subscriptions = append(s.subscriptions, newSubscription(topic, handler))
}

func NewAmqpSubscriber(connectionManager *AmqpConnectionManager, queue string) (*AmqpSubscriber, error) {
	return &AmqpSubscriber{
		connectionManager: connectionManager,
		queue:             queue,
		exclusive:         false,
	}, nil
}

func NewExclusiveAmqpSubscriber(connectionManager *AmqpConnectionManager, queue string) (*AmqpSubscriber, error) {
	return &AmqpSubscriber{
		connectionManager: connectionManager,
		queue:             queue,
		exclusive:         true,
	}, nil
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
		isConnected:      false,
		isReconnecting:   true,
		connectionUpdate: make(chan bool),
	}

	go func() {

		// go func(am *AmqpConnectionManager) {
		// 	time.Sleep(10100 * time.Millisecond)
		// 	am.connection.Close()
		// }(amqpConnectionManager)

		for i := 0; i < 50; i++ {
			logger.Printf("> attempting to connect: %#v", amqpConnectionManager)

			connection, err := amqp.Dial("amqp://" + amqpConnectionManager.user + ":" + amqpConnectionManager.pass + "@" + amqpConnectionManager.host + ":" + amqpConnectionManager.port + "/" + amqpConnectionManager.virtualHost)
			if err != nil {
				logger.Println("> failed to connect:", err)
				time.Sleep(time.Duration((i%5)+1) * time.Second)
				continue
			}

			logger.Println("We now have a connection, we should rebind now!")

			amqpConnectionManager.connection = connection
			amqpConnectionManager.isConnected = true
			amqpConnectionManager.isReconnecting = false
			close(amqpConnectionManager.connectionUpdate)

			<-connection.NotifyClose(make(chan *amqp.Error, 0))
			logger.Println("Our connection has been broken, attempting to reconnect")

			// panic("HAD TO RECONNECT")

			// Reset i to attempt to reconnect 50 times again
			i = 0

			amqpConnectionManager.connection = nil
			amqpConnectionManager.isConnected = false
			amqpConnectionManager.isReconnecting = true
			amqpConnectionManager.connectionUpdate = make(chan bool)
		}

		// 50 attempts for a single connection have failed, mark as permanent failure
		amqpConnectionManager.isConnected = false
		amqpConnectionManager.isReconnecting = false
	}()

	return amqpConnectionManager
}

//this should shoot off our reconnecting logic when the connection recieves the close signal
func (cm *AmqpConnectionManager) CloseConnection() {
	logger.Println("[AmqpConnectionManager.CloseConnection] We are attempting to manually close the connection.")

	if cm.connection != nil && cm.isConnected && !cm.isReconnecting {
		cm.isConnected = false
		cm.isReconnecting = true
		cm.connection.Close()

		logger.Println("[AmqpConnectionManager.CloseConnection] We have manually closed the connection.")
	} else {
		logger.Println("[AmqpConnectionManager.CloseConnection] We cannot close a disconnection connection.")
	}
}
