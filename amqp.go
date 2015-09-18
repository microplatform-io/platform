package platform

import (
	"errors"
	"net"
	"time"

	"github.com/streadway/amqp"
)

const PUBLISH_RETRIES = 3
const MAX_WORKERS = 20

type AmqpPublisher struct {
	connectionManager *AmqpConnectionManager
	channel           *amqp.Channel
}

func subscriptionWorker(subscription *subscription, workQueue chan amqp.Delivery) {
	for {
		for work := range workQueue {
			subscriberDoWork(subscription, work)
		}
	}
}

func subscriberDoWork(subscription *subscription, msg amqp.Delivery) {
	subscription.Handler.HandleMessage(msg.Body)

	msg.Ack(false)
}

func (p *AmqpPublisher) Publish(topic string, body []byte) error {
	if p.channel == nil {
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
			return nil
		}

		// Give it a chance to breathe
		time.Sleep(10 * time.Millisecond)

		p.resetChannel()
	}

	return publishErr
}

func (p *AmqpPublisher) resetChannel() error {
	connection, err := p.connectionManager.GetConnection(false)
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
	Topic       string
	Handler     ConsumerHandler
	Concurrency int
	workQueue   chan amqp.Delivery
}

type AmqpSubscriber struct {
	connectionManager *AmqpConnectionManager
	queue             string
	subscriptions     []*subscription
}

func (s *AmqpSubscriber) Run() error {
	logger.Printf("> AmqpSubscriber.Run: initiating")

	for {
		if err := s.run(); err != nil {
			logger.Printf("> failed to run subscription: %s", err)
		}
	}

	return nil
}

func (s *AmqpSubscriber) run() error {
	logger.Printf("> AmqpSubscriber.run: grabbing connection")

	conn, err := s.connectionManager.GetConnection(true)
	if err != nil {
		return err
	}

	logger.Printf("> AmqpSubscriber.run: got connection: %s", conn)

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	logger.Printf("> AmqpSubscriber.run: got channel: %s", conn)

	if _, err := ch.QueueDeclare(s.queue, false, true, false, false, nil); err != nil {
		return err
	}

	for _, subscription := range s.subscriptions {
		logger.Println("> binding subscription")
		if err := ch.QueueBind(s.queue, subscription.Topic, "amq.topic", false, nil); err != nil {
			return err
		}

		subscription.workQueue = make(chan amqp.Delivery)

		for i := 0; i <= subscription.Concurrency; i++ {
			go subscriptionWorker(subscription, subscription.workQueue)
		}
	}

	msgs, err := ch.Consume(
		s.queue, // queue
		"",      // consumer defined by server
		false,   // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	if err != nil {
		return err
	}

	for msg := range msgs {

		handled := false

		for _, subscription := range s.subscriptions {
			if subscription.Topic == "" || (subscription.Topic == msg.RoutingKey) {

				select {
				case subscription.workQueue <- msg:
					handled = true

				case <-time.After(500 * time.Millisecond):
					// ignore
				}
			}
		}

		if !handled {
			msg.Reject(true)
		}
	}

	return errors.New("connection has been closed")
}

func (s *AmqpSubscriber) Subscribe(topic string, handler ConsumerHandler, concurrency int) {
	s.subscriptions = append(s.subscriptions, &subscription{
		Topic:       topic,
		Handler:     handler,
		Concurrency: concurrency,
	})
}

func NewAmqpSubscriber(connectionManager *AmqpConnectionManager, queue string) (*AmqpSubscriber, error) {
	return &AmqpSubscriber{
		connectionManager: connectionManager,
		queue:             queue,
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
		for i := 0; i < 50; i++ {
			logger.Printf("> attempting to connect: %s %#v", &amqpConnectionManager, amqpConnectionManager)

			connection, err := amqp.Dial("amqp://" + amqpConnectionManager.user + ":" + amqpConnectionManager.pass + "@" + amqpConnectionManager.host + ":" + amqpConnectionManager.port + "/" + amqpConnectionManager.virtualHost)
			if err != nil {
				logger.Println("> failed to connect:", err)
				time.Sleep(time.Duration((i%5)+1) * time.Second)
				continue
			}

			amqpConnectionManager.connection = connection
			amqpConnectionManager.isConnected = true
			amqpConnectionManager.isReconnecting = false
			close(amqpConnectionManager.connectionUpdate)

			<-connection.NotifyClose(make(chan *amqp.Error, 0))

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
