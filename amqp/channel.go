package amqp

import (
	"errors"
	"time"

	"github.com/streadway/amqp"
)

type ChannelInterface interface {
	Close() error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan DeliveryInterface, error)
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
}

type Channel struct {
	channel *amqp.Channel
}

func (ch *Channel) Close() error {
	return ch.channel.Close()
}

func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan DeliveryInterface, error) {
	deliveries, err := ch.channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	if err != nil {
		return nil, err
	}

	deliveriesInterfaceChan := make(chan DeliveryInterface)
	go func() {
		for d := range deliveries {
			deliveriesInterfaceChan <- Delivery{d}
		}
	}()

	return (<-chan DeliveryInterface)(deliveriesInterfaceChan), nil
}

func (ch *Channel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	return ch.channel.NotifyClose(c)
}

func (ch *Channel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return ch.channel.Publish(exchange, key, mandatory, immediate, msg)
}

func (ch *Channel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return ch.channel.QueueBind(name, key, exchange, noWait, args)
}

func (ch *Channel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return ch.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

// MOCKS

type mockConsume struct {
	queue     string
	consumer  string
	autoAck   bool
	exclusive bool
	noLocal   bool
	noWait    bool
	args      amqp.Table
}

type mockPublish struct {
	exchange  string
	key       string
	mandatory bool
	immediate bool
	msg       amqp.Publishing
}

type mockQueueBind struct {
	name     string
	key      string
	exchange string
	noWait   bool
	args     amqp.Table
}

type mockQueueDeclare struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
	args       amqp.Table
}

type mockChannel struct {
	notifyCloses      []chan *amqp.Error
	mockConsumes      []mockConsume
	mockPublishes     []mockPublish
	mockQueueBinds    []mockQueueBind
	mockQueueDeclares []mockQueueDeclare

	closed             bool
	mockDeliveries     chan DeliveryInterface
	publishErrors      []error
	queueBindErrors    []error
	queueDeclareErrors []error
}

func (ch *mockChannel) Close() error {
	if !ch.closed {
		ch.closed = true

		for i := range ch.notifyCloses {
			close(ch.notifyCloses[i])
		}

		ch.notifyCloses = []chan *amqp.Error{}
	}

	return nil
}

func (ch *mockChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan DeliveryInterface, error) {
	if ch.closed {
		return nil, errors.New("mock channel has been closed")
	}

	ch.mockConsumes = append(ch.mockConsumes, mockConsume{
		queue:     queue,
		consumer:  consumer,
		autoAck:   autoAck,
		exclusive: exclusive,
		noLocal:   noLocal,
		noWait:    noWait,
		args:      args,
	})

	return ch.mockDeliveries, nil
}

func (ch *mockChannel) errorOnPublish(err error) *mockChannel {
	ch.publishErrors = append(ch.publishErrors, err)

	return ch
}

func (ch *mockChannel) errorOnQueueBind(err error) *mockChannel {
	ch.queueBindErrors = append(ch.queueBindErrors, err)

	return ch
}

func (ch *mockChannel) errorOnQueueDeclare(err error) *mockChannel {
	ch.queueDeclareErrors = append(ch.queueDeclareErrors, err)

	return ch
}

func (ch *mockChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	ch.notifyCloses = append(ch.notifyCloses, c)

	return c
}

func (ch *mockChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if ch.closed {
		return errors.New("mock channel has been closed")
	}

	// Simulate a tiny amount of latency
	time.Sleep(1 * time.Millisecond)

	if len(ch.publishErrors) > 0 {
		publishError := ch.publishErrors[0]

		ch.publishErrors = ch.publishErrors[1:]

		return publishError
	}

	ch.mockPublishes = append(ch.mockPublishes, mockPublish{
		exchange:  exchange,
		key:       key,
		mandatory: mandatory,
		immediate: immediate,
		msg:       msg,
	})

	return nil
}

func (ch *mockChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	if ch.closed {
		return errors.New("mock channel has been closed")
	}

	if len(ch.queueBindErrors) > 0 {
		queueBindError := ch.queueBindErrors[0]

		ch.queueBindErrors = ch.queueBindErrors[1:]

		return queueBindError
	}

	ch.mockQueueBinds = append(ch.mockQueueBinds, mockQueueBind{
		name:     name,
		key:      key,
		exchange: exchange,
		noWait:   noWait,
		args:     args,
	})

	return nil
}

func (ch *mockChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	if ch.closed {
		return amqp.Queue{}, errors.New("mock channel has been closed")
	}

	if len(ch.queueDeclareErrors) > 0 {
		queueDeclareError := ch.queueDeclareErrors[0]

		ch.queueDeclareErrors = ch.queueDeclareErrors[1:]

		return amqp.Queue{}, queueDeclareError
	}

	ch.mockQueueDeclares = append(ch.mockQueueDeclares, mockQueueDeclare{
		name:       name,
		durable:    durable,
		autoDelete: autoDelete,
		exclusive:  exclusive,
		noWait:     noWait,
		args:       args,
	})

	return amqp.Queue{}, nil
}

func newMockChannel() *mockChannel {
	return &mockChannel{
		notifyCloses:      []chan *amqp.Error{},
		mockConsumes:      []mockConsume{},
		mockPublishes:     []mockPublish{},
		mockQueueBinds:    []mockQueueBind{},
		mockQueueDeclares: []mockQueueDeclare{},

		mockDeliveries: make(chan DeliveryInterface),
	}
}
