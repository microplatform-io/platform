package amqp

import (
	"errors"
	"time"

	"github.com/Sirupsen/logrus"
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

// This method will use the subscriber's queue name as the queue to bind to
// and then manage the binding of that queue to every subscription bound to
// this subscriber.
func (s *Subscriber) queueBind(channelInterface ChannelInterface) error {
	if channelInterface == nil {
		return errors.New("Nil channel interface")
	}

	for i := range s.subscriptions {
		logger.WithFields(logrus.Fields{
			"queue": s.queue,
			"topic": s.subscriptions[i].topic,
		}).Debug("binding the queue to a topic")

		if err := channelInterface.QueueBind(s.queue, s.subscriptions[i].topic, "amq.topic", false, nil); err != nil {
			return err
		}
	}

	return nil
}

// Handles the various flags that could be set on the subscriber and properly
// declares the queue on the channel interface. The creation of the channel
// interface is not this method's responsibility.
func (s *Subscriber) queueDeclare(channelInterface ChannelInterface) error {
	if channelInterface == nil {
		return errors.New("Nil channel interface")
	}

	durable := true
	if s.exclusive {
		durable = false
	}

	_, err := channelInterface.QueueDeclare(s.queue, durable, s.autoDelete, s.exclusive, false, nil)

	return err
}

// Our running of the actual subscriber, where we declare and bind based on the notion of the
// subscriber and handles the messages. If we receive a signal that the channel interface is closed
// we will break out and wait for a new connection.
func (s *Subscriber) run() error {
	entry := logger.WithField("method", "Subscriber.run")

	entry.Info("Getting a connection interface")

	connectionInterface, err := s.dialerInterface.Dial()
	if err != nil {
		entry.WithError(err).Error("Failed to get a connection interface")
		return err
	}

	connectionClosed := connectionInterface.NotifyClose(make(chan *amqp.Error))

	entry.WithField("connection_interface", connectionInterface).Info("Getting a channel interface")

	channelInterface, err := connectionInterface.GetChannelInterface()
	if err != nil {
		entry.WithError(err).Error("Failed to get a channel interface")
		return err
	}

	channelInterfaceClosed := channelInterface.NotifyClose(make(chan *amqp.Error))

	entry.Info("Declaring the queue")

	if err := s.queueDeclare(channelInterface); err != nil {
		entry.WithError(err).Error("Failed to declare the queue")
		return err
	}

	entry.Info("Binding the queue")

	if err := s.queueBind(channelInterface); err != nil {
		entry.WithError(err).Error("Failed to bind the queue")
		return err
	}

	entry.Info("Consuming messages from the channel interface")

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
		entry.WithError(err).Error("Failed to consume messages from the channel interface")
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
			couldHandle := false
			wasHandled := false

			for _, subscription := range s.subscriptions {
				if subscription.canHandle(msg) {
					couldHandle = true

					select {
					case subscription.deliveries <- msg:
						wasHandled = true

					default:
					}
				}
			}

			if couldHandle {
				if wasHandled {
					msg.Ack(false)
				} else {
					msg.Reject(true)

					entry.WithField("message", msg).WithField("reason", "busy").Warn("Failed to handle a message to this subscriber")
				}
			} else {
				msg.Ack(false)

				entry.WithField("message", msg).WithField("reason", "undeliverable").Error("Failed to handle a message to this subscriber")
			}

		case err := <-connectionClosed:
			if err != nil {
				entry.WithError(err).Error("The connection has been closed")
			} else {
				entry.Info("The connection has been closed")
			}
			iterate = false

		case err := <-channelInterfaceClosed:
			if err != nil {
				entry.WithError(err).Error("The channel has been closed")
			} else {
				entry.Info("The channel has been closed")
			}
			iterate = false

		case <-s.quit:
			if err != nil {
				entry.WithError(err).Error("The subscriber has been closed")
			} else {
				entry.Info("The subscriber has been closed")
			}
			iterate = false

			return subscriberClosed
		}
	}

	return nil
}

func (s *Subscriber) Run() {
	entry := logger.WithField("method", "Subscriber.run")

	entry.Debug("Initiating the run goroutine")

	s.started = make(chan interface{})

	go func() {
		for {
			entry.Debug("Calling private run method")

			if err := s.run(); err != nil {
				if err == subscriberClosed {
					return
				}

				entry.WithError(err).Error("failed to run subscription: %s", err)
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
