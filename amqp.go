package platform

import (
	"errors"
	"github.com/streadway/amqp"
	"math/rand"
	"net"
	"sync"
	"time"
)

const PUBLISH_RETRIES = 3
const MAX_WORKERS = 20

type AmqpPublisher struct {
	connectionManager *AmqpConnectionManager
	channel           *amqp.Channel
	exchange          string
}

func subscriptionWorker(subscription *subscription, workQueue chan amqp.Delivery) {
	for {
		for work := range workQueue {
			subscriberDoWork(subscription, work)
		}
	}
}

func subscriberDoWork(subscription *subscription, msg amqp.Delivery) {
	if err := subscription.Handler.HandleMessage(msg.Body); err != nil {
		// If this message has already been redelivered once, just ack it to discard it
		if msg.Redelivered {
			msg.Ack(true)
		} else {
			msg.Reject(true)
		}
	} else {
		msg.Ack(true)
	}
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
			p.exchange, // exchange
			topic,      // routing key
			false,      // mandatory
			false,      // immediate
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
	return &AmqpPublisher{
		connectionManager: connectionManager,
		exchange:          "amq.topic",
	}, nil
}

func NewCustomAmqpPublisher(connectionManager *AmqpConnectionManager, exchange string) (*AmqpPublisher, error) {
	return &AmqpPublisher{
		connectionManager: connectionManager,
		exchange:          exchange,
	}, nil
}

type MultiPublisher struct {
	publishers    []Publisher
	nextPublisher int
}

func (p *MultiPublisher) Publish(topic string, body []byte) error {
	var publishErr error

	for i := 0; i < len(p.publishers); i++ {
		publishErr = p.publishers[p.nextPublisher].Publish(topic, body)

		p.nextPublisher = (p.nextPublisher + 1) % len(p.publishers)

		if publishErr == nil {
			return nil
		}
	}

	return publishErr
}

func NewMultiPublisher(publishers ...Publisher) Publisher {
	p := make([]Publisher, len(publishers))
	copy(p, publishers)

	rand.Seed(time.Now().UTC().UnixNano())
	ptr := rand.Intn(len(publishers))

	return &MultiPublisher{
		publishers:    p,
		nextPublisher: ptr,
	}
}

type subscription struct {
	Topic       string
	Handler     ConsumerHandler
	Concurrency int
	workQueue   chan amqp.Delivery
}

type AmqpQueue struct {
	Name             string
	Durable          bool
	DeleteWhenUnused bool
}

type AmqpExchange struct {
	Name         string
	ExchangeType string
}

type AmqpSubscriber struct {
	connectionManager *AmqpConnectionManager
	subscriptions     []*subscription
	tag               string
	declareExchange   bool
	exchange          *AmqpExchange
	queue             *AmqpQueue
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

	if s.declareExchange {
		logger.Printf("> AmqpSubscriber.run: got Channel, declaring Exchange (%q)", s.exchange)
		if err = ch.ExchangeDeclare(
			s.exchange.Name,         // name of the exchange
			s.exchange.ExchangeType, // type
			true,  // durable
			false, // delete when complete
			false, // internal
			false, // noWait
			nil,   // arguments
		); err != nil {
			logger.Printf("> Err declaring exchange: %s", err)
			return err
		}
	}

	logger.Printf("> AmqpSubscriber.run: got channel: %s", conn)

	logger.Printf("> AmqpSubscriber.run: declared Exchange, declaring Queue %q", s.queue)
	qu := amqp.Queue{}
	if qu, err = ch.QueueDeclare(
		s.queue.Name,
		s.queue.Durable,
		s.queue.DeleteWhenUnused,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	for _, subscription := range s.subscriptions {
		logger.Printf("> AmqpSubscriber.run: declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
			s.queue, qu.Messages, qu.Consumers, subscription.Topic)
		if err := ch.QueueBind(
			s.queue.Name,
			subscription.Topic,
			s.exchange.Name,
			false,
			nil,
		); err != nil {
			return err
		}

		subscription.workQueue = make(chan amqp.Delivery)

		for i := 0; i <= subscription.Concurrency; i++ {
			go subscriptionWorker(subscription, subscription.workQueue)
		}
	}

	logger.Printf("> consuming: %s, %s", s.queue, s.tag)
	msgs, err := ch.Consume(
		s.queue.Name, // queue
		s.tag,        // consumer defined by server
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
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

func NewAmqpSubscriber(connectionManager *AmqpConnectionManager, queueName string) (*AmqpSubscriber, error) {
	return &AmqpSubscriber{
		connectionManager: connectionManager,
		queue: &AmqpQueue{
			Name:             queueName,
			Durable:          false,
			DeleteWhenUnused: true,
		},
		exchange: &AmqpExchange{
			Name:         "amq.topic",
			ExchangeType: "",
		},
		tag:             "",
		declareExchange: false,
	}, nil
}

func NewCustomAmqpSubscriber(connectionManager *AmqpConnectionManager, queue *AmqpQueue, exchange *AmqpExchange, tag string) (*AmqpSubscriber, error) {
	return &AmqpSubscriber{
		connectionManager: connectionManager,
		queue:             queue,
		exchange:          exchange,
		tag:               tag,
		declareExchange:   true,
	}, nil
}

type MultiSubscriber struct {
	subscribers []Subscriber
}

func (s *MultiSubscriber) Run() error {
	wg := sync.WaitGroup{}
	wg.Add(len(s.subscribers))

	for i := range s.subscribers {
		go func(i int) {
			s.subscribers[i].Run()
			wg.Done()
		}(i)
	}

	wg.Wait()

	return nil
}

func (s *MultiSubscriber) Subscribe(topic string, handler ConsumerHandler, concurrency int) {
	for _, subscriber := range s.subscribers {
		subscriber.Subscribe(topic, handler, concurrency)
	}
}

func NewMultiSubscriber(subscribers ...Subscriber) Subscriber {
	s := make([]Subscriber, len(subscribers))
	copy(s, subscribers)

	return &MultiSubscriber{s}
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
