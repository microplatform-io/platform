package amqp

import (
	"errors"
	"time"

	"github.com/microplatform-io/platform"
	"github.com/streadway/amqp"
)

var subscriberClosed = errors.New("subscriber has been closed")

type Subscriber struct {
	dialerInterface DialerInterface
	subscriptions   []*subscription
	started         chan interface{}
	closed          bool
	quit            chan interface{}

	// Queue properties
	queue      string
	exclusive  bool
	autoDelete bool
}

func (s *Subscriber) Close() error {
	if !s.closed {
		for i := range s.subscriptions {
			s.subscriptions[i].Close()
		}

		close(s.quit)

		s.closed = true
	}

	return nil
}

// Our running of the actual subscriber, where we declare and bind based on the notion of the
// subscriber and handles the messages. If we recieve a signal that the channel interface is closed
// we will break out and wait for a new connection.
func (s *Subscriber) run() error {
	logger.Printf("[Subscriber.run] bootstrapping connection")

	connectionInterface, err := s.dialerInterface.Dial()
	if err != nil {
		logger.Printf("[Subscriber.run] failed to get a connection interface: %s", err)
		return err
	}

	connectionClosed := connectionInterface.NotifyClose(make(chan *amqp.Error))

	logger.Println("[Subscriber.run] bootstrapping channel using:", connectionInterface)

	channelInterface, err := connectionInterface.GetChannelInterface()
	if err != nil {
		logger.Printf("[Subscriber.run] failed to get a channel interface: %s", err)
		return err
	}

	channelInterfaceClosed := channelInterface.NotifyClose(make(chan *amqp.Error))

	durable := true
	autoDelete := false

	if s.exclusive {
		durable = false
	}

	if s.autoDelete {
		autoDelete = true
	}

	logger.Printf("[Subscriber.run] bootstrapping queue")

	if _, err := channelInterface.QueueDeclare(s.queue, durable, autoDelete, s.exclusive, false, nil); err != nil {
		logger.Printf("[Subscriber.run] failed to declare a queue: %s", err)
		return err
	}

	logger.Printf("[Subscriber.run] bootstrapping queue bindings")

	for _, subscription := range s.subscriptions {
		logger.Infoln("binding", s.queue, "to", subscription.topic)
		if err := channelInterface.QueueBind(s.queue, subscription.topic, "amq.topic", false, nil); err != nil {
			logger.Errorf("[Subscriber.run] failed to bind topic to a queue: %s", err)
			return err
		}
	}

	logger.Println("[Subscriber.run] After finished binding")

	msgs, err := channelInterface.Consume(
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

	if s.started != nil {
		close(s.started)
		s.started = nil
	}

	for iterate {
		select {
		case msg := <-msgs:
			handled := false

			for _, subscription := range s.subscriptions {
				if subscription.canHandle(msg) {
					select {
					case subscription.deliveries <- msg:
						handled = true

					default:
					}
				}
			}

			if !handled {
				msg.Reject(true)
			}

		case err := <-connectionClosed:
			logger.Println("[Subscriber.run] An event occurred causing the need for a new connection : ", err)
			iterate = false

		case err := <-channelInterfaceClosed:
			logger.Println("[Subscriber.run] An event occurred causing the need for a new channelInterface : ", err)
			iterate = false

		case <-s.quit:
			logger.Println("[Subscriber.run] subscriber has been closed")
			iterate = false

			return subscriberClosed
		}
	}

	return nil
}

func (s *Subscriber) Run() {
	logger.Printf("[Subscriber.Run] initiating")

	s.started = make(chan interface{})

	go func() {
		for {
			logger.Println("[Subscriber.Run] calling private run method.")

			if err := s.run(); err != nil {
				if err == subscriberClosed {
					return
				}

				logger.Errorf("[Subscriber.Run] failed to run subscription: %s", err)
			}

			// Sleeping for a few milliseconds allows the close event to propagate everywhere
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Wait for everything to be bound
	<-s.started
}

func (s *Subscriber) Subscribe(topic string, handler platform.ConsumerHandler) {
	s.subscriptions = append(s.subscriptions, newSubscription(topic, handler))
}

func NewSubscriber(dialerInterface DialerInterface, queue string) (*Subscriber, error) {
	return &Subscriber{
		dialerInterface: dialerInterface,
		quit:            make(chan interface{}),
		queue:           queue,
		exclusive:       false,
		autoDelete:      false,
	}, nil
}

func NewMultiSubscriber(dialerInterfaces []DialerInterface, queue string) (platform.Subscriber, error) {
	subscribers := make([]platform.Subscriber, len(dialerInterfaces))

	for i := range dialerInterfaces {
		subscriber, err := NewSubscriber(dialerInterfaces[i], queue)
		if err != nil {
			return nil, err
		}

		subscribers[i] = subscriber
	}

	return platform.NewMultiSubscriber(subscribers), nil
}

func NewExclusiveSubscriber(dialerInterface DialerInterface, queue string) (*Subscriber, error) {
	return &Subscriber{
		dialerInterface: dialerInterface,
		quit:            make(chan interface{}),
		queue:           queue,
		exclusive:       true,
		autoDelete:      false,
	}, nil
}

func NewAutoDeleteSubscriber(dialerInterface DialerInterface, queue string) (*Subscriber, error) {
	return &Subscriber{
		dialerInterface: dialerInterface,
		quit:            make(chan interface{}),
		queue:           queue,
		exclusive:       false,
		autoDelete:      true,
	}, nil
}
