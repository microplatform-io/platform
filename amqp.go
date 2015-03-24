package platform

import (
	"errors"
	"fmt"
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
	ch      *amqp.Channel
}

type MultiAmqpSubscription struct {
	amqpSubscriptions []*AmqpSubscription
}

func (s *MultiAmqpSubscription) Run() error {
	var wg sync.WaitGroup

	wg.Add(len(s.amqpSubscriptions))

	for i := range s.amqpSubscriptions {
		go func(i int) {
			s.amqpSubscriptions[i].Run()
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
	defer s.ch.Close()

	_, err := s.ch.QueueDeclare(s.queue, false, true, false, false, nil)
	if err != nil {
		return err
	}

	if err := s.ch.QueueBind(s.queue, s.topic, "amq.topic", false, nil); err != nil {
		return err
	}

	msgs, err := s.ch.Consume(
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

	for {
		for msg := range msgs {
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
		}
	}

	return nil
}

type AmqpPublisher struct {
	connMgr *AmqpConnectionManager
}

type MultiAmqpPublisher struct {
	connMgrs   []*AmqpConnectionManager
	connMgrPtr int
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

func (p *MultiAmqpPublisher) Publish(topic string, body []byte) error {
	fmt.Printf("[amqp] > publishing for %s", topic)

	conn, err := p.connMgrs[p.connMgrPtr].GetConnection()
	if err != nil {
		return err
	}
	fmt.Printf("[amqp] > publishing on %+v", p.connMgrs[p.connMgrPtr])

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	p.connMgrPtr = (p.connMgrPtr + 1) % len(p.connMgrs)

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

func NewAmqpPublisher(connMgr *AmqpConnectionManager) (*AmqpPublisher, error) {
	_, err := connMgr.GetConnection()
	if err != nil {
		return nil, err
	}

	return &AmqpPublisher{connMgr: connMgr}, nil
}

func NewMultiAmqpPublisher(connMgrs []*AmqpConnectionManager) (*MultiAmqpPublisher, error) {
	mgrs := []*AmqpConnectionManager{}

	for _, connMgr := range connMgrs {
		_, err := connMgr.GetConnection()
		if err != nil {
			//
			continue
		}

		mgrs = append(mgrs, connMgr)
	}

	if len(mgrs) == 0 {
		return nil, errors.New("Could not make a single connection to publish")
	}

	// randomly assign the ptr to one of the connected mgrs instead of starting
	// them all at 0
	rand.Seed(time.Now().UTC().UnixNano())

	return &MultiAmqpPublisher{
		connMgrs:   mgrs,
		connMgrPtr: rand.Intn(len(mgrs)),
	}, nil

}

type MultiAmqpSubscriber struct {
	connMgrs []*AmqpConnectionManager
	queue    string
}

type AmqpSubscriber struct {
	connMgr *AmqpConnectionManager
	queue   string
}

func (s *AmqpSubscriber) Subscribe(topic string, handler ConsumerHandler) (Subscription, error) {
	conn, err := s.connMgr.GetConnection()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &AmqpSubscription{
		queue:   s.queue,
		topic:   topic,
		ch:      ch,
		handler: handler,
	}, nil
}

func (s *MultiAmqpSubscriber) Subscribe(topic string, handler ConsumerHandler) (Subscription, error) {
	amqpSubscriptions := []*AmqpSubscription{}

	for _, connMgr := range s.connMgrs {
		conn, err := connMgr.GetConnection()
		if err != nil {
			//return nil, err
			continue
		}

		ch, err := conn.Channel()
		if err != nil {
			//return nil, err
			continue
		}

		amqpSubscriptions = append(amqpSubscriptions, &AmqpSubscription{
			queue:   s.queue,
			topic:   topic,
			ch:      ch,
			handler: handler,
		})
	}

	if len(amqpSubscriptions) == 0 {
		return nil, errors.New("Could not create a single subscription.")
	}

	return &MultiAmqpSubscription{
		amqpSubscriptions: amqpSubscriptions,
	}, nil
}

func NewMultiAmqpSubscriber(connMgrs []*AmqpConnectionManager, queue string) (*MultiAmqpSubscriber, error) {

	mgrs := []*AmqpConnectionManager{}

	for _, connMgr := range connMgrs {
		_, err := NewAmqpSubscriber(connMgr, queue)
		if err != nil {
			//Issue connecting to this rabbitmq.
			continue
		}

		mgrs = append(mgrs, connMgr)
	}

	if len(mgrs) == 0 {
		return nil, errors.New("Could not create a single subscriber.")
	}

	return &MultiAmqpSubscriber{
		connMgrs: mgrs,
		queue:    queue,
	}, nil
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
