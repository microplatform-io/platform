package platform

import (
	"errors"
	"github.com/streadway/amqp"
	"math/rand"
	"net"
	"sync"
	"time"
)

// The amqp subscription acts as an isolated unit of code that can
// run the range over messages that the amqp channel has to offer.
type AmqpSubscription struct {
	queue   string
	topic   string
	handler ConsumerHandler
	connMgr *AmqpConnectionManager
}

type MultiSubscription struct {
	subscriptions []Subscription
}

func (s *MultiSubscription) Run() error {
	var wg sync.WaitGroup

	wg.Add(len(s.subscriptions))

	for i := range s.subscriptions {
		go func(i int) {
			s.subscriptions[i].Run()
			wg.Done()
		}(i)
	}

	wg.Wait()
	return nil
}

// Range over the channel's messages after declaring the queue and binding
// it to the exchange and topic. This function blocks permanently, so consider
// invoking it inside of a goroutine
func (s *AmqpSubscription) Run() error {
	for {
		conn, err := s.connMgr.GetConnection()
		if err != nil {
			return err
		}

		ch, err := conn.Channel()
		if err != nil {
			return err
		}

		s.run(ch)
	}

	return nil
}

func (s *AmqpSubscription) run(ch *amqp.Channel) error {
	logger.Println("> subscription is being bound")

	defer ch.Close()

	_, err := ch.QueueDeclare(s.queue, false, true, false, false, nil)
	if err != nil {
		return err
	}

	if err := ch.QueueBind(s.queue, s.topic, "amq.topic", false, nil); err != nil {
		return err
	}

	msgs, err := ch.Consume(
		s.queue, // queue
		s.topic, // consumer
		false,   // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	if err != nil {
		return err
	}

	logger.Println("> subscription has been bound")

	for {
		select {
		case msg := <-msgs:
			if s.topic == "" || (s.topic == msg.RoutingKey) {
				if err := s.handler.HandleMessage(msg.Body); err != nil {
					// If this message has already been redelivered once, just ack it to discard it
					if msg.Redelivered {
						msg.Ack(true)
					} else {
						msg.Reject(true)
					}
				} else {
					msg.Ack(true)
				}
			} else {
				msg.Reject(true)
			}

		case <-s.connMgr.conn.NotifyClose(make(chan *amqp.Error)):
			break
		}
	}

	logger.Println("> subscription has returned")

	return nil
}

type AmqpPublisher struct {
	connMgr *AmqpConnectionManager
}

type MultiPublisher struct {
	publishers    []Publisher
	nextPublisher int
}

func (p *AmqpPublisher) Publish(topic string, body []byte) error {
	logger.Printf("[amqp] > publishing for %s", topic)

	conn, err := p.connMgr.GetConnection()
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return ch.Publish(
		"amq.topic", // exchange
		topic,       // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		},
	)
}

func (p *MultiPublisher) Publish(topic string, body []byte) error {
	err := p.publishers[p.nextPublisher].Publish(topic, body)
	if err != nil {
		return err
	}

	p.nextPublisher = (p.nextPublisher + 1) % len(p.publishers)
	return nil
}

func NewAmqpPublisher(connMgr *AmqpConnectionManager) (*AmqpPublisher, error) {
	_, err := connMgr.GetConnection()
	if err != nil {
		return nil, err
	}

	return &AmqpPublisher{connMgr: connMgr}, nil
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

type MultiSubscriber struct {
	subscribers []Subscriber
}

type AmqpSubscriber struct {
	connMgr *AmqpConnectionManager
	queue   string
}

func (s *AmqpSubscriber) Subscribe(topic string, handler ConsumerHandler) (Subscription, error) {
	return &AmqpSubscription{
		queue:   s.queue,
		topic:   topic,
		handler: handler,
		connMgr: s.connMgr,
	}, nil
}

func (s *MultiSubscriber) Subscribe(topic string, handler ConsumerHandler) (Subscription, error) {
	subscriptions := []Subscription{}

	for _, subscriber := range s.subscribers {
		sub, err := subscriber.Subscribe(topic, handler)
		if err != nil {
			continue
		}

		subscriptions = append(subscriptions, sub)
	}

	if len(subscriptions) == 0 {
		return nil, errors.New("Could not create one or more subsciptions")
	}

	return &MultiSubscription{
		subscriptions: subscriptions,
	}, nil
}

func NewMultiSubscriber(subscribers ...Subscriber) Subscriber {
	s := make([]Subscriber, len(subscribers))
	copy(s, subscribers)

	return &MultiSubscriber{s}
}

func NewAmqpSubscriber(connMgr *AmqpConnectionManager, queue string) (*AmqpSubscriber, error) {
	_, err := connMgr.GetConnection()
	if err != nil {
		return nil, err
	}

	return &AmqpSubscriber{
		connMgr: connMgr,
		queue:   queue,
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

	conn *amqp.Connection
}

// Return the existing connection if one has already been established, or
// else we should generate a new connection and cache it for reuse.
func (cm *AmqpConnectionManager) GetConnection() (*amqp.Connection, error) {
	if cm.conn != nil {
		return cm.conn, nil
	}

	conn, err := amqp.Dial("amqp://" + cm.user + ":" + cm.pass + "@" + cm.host + ":" + cm.port + "/" + cm.virtualHost)
	if err != nil {
		return nil, err
	}

	cm.conn = conn

	go func() {
		amqpErr := <-conn.NotifyClose(make(chan *amqp.Error, 0))

		logger.Println("> connection has been closed:", amqpErr)

		for i := 0; i < 5; i++ {
			logger.Println("> attempting to reconnect...")

			time.Sleep(time.Duration(i+1) * 5 * time.Second)

			if _, err := cm.GetConnection(); err != nil {
				logger.Println("> failed to reconnect:", err)
				continue
			}

			logger.Println("> reconnected!")

			break
		}
	}()

	return cm.conn, nil
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

	return &AmqpConnectionManager{
		user:        user,
		pass:        pass,
		host:        host,
		port:        port,
		virtualHost: virtualHost,
	}
}
