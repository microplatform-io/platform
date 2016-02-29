package amqp

import (
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type DialerInterface interface {
	Dial() (ConnectionInterface, error)
}

type Dialer struct {
	url string
}

func (d *Dialer) Dial() (ConnectionInterface, error) {
	connection, err := amqp.Dial(d.url)
	if err != nil {
		return nil, err
	}

	return &Connection{
		connection: connection,
	}, nil
}

func NewDialer(url string) *Dialer {
	return &Dialer{url: url}
}

func NewDialers(urls []string) []*Dialer {
	amqpDialers := []*Dialer{}

	for i := range urls {
		amqpDialers = append(amqpDialers, NewDialer(urls[i]))
	}

	return amqpDialers
}

type CachingDialer struct {
	dialer     *Dialer
	connection ConnectionInterface
	mu         sync.Mutex
}

func (d *CachingDialer) Dial() (ConnectionInterface, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.connection != nil {
		return d.connection, nil
	}

	connection, err := d.dialer.Dial()
	if err != nil {
		return nil, err
	}

	d.connection = connection

	go d.keepAlive()

	return connection, nil
}

func (d *CachingDialer) keepAlive() {
	d.mu.Lock()
	closed := d.connection.NotifyClose(make(chan *amqp.Error))
	d.mu.Unlock()

	<-closed

	d.mu.Lock()
	d.connection = nil
	d.mu.Unlock()
}

func NewCachingDialer(url string) *CachingDialer {
	return &CachingDialer{
		dialer: NewDialer(url),
	}
}

func NewCachingDialers(urls []string) []*CachingDialer {
	CachingamqpDialers := []*CachingDialer{}

	for i := range urls {
		CachingamqpDialers = append(CachingamqpDialers, NewCachingDialer(urls[i]))
	}

	return CachingamqpDialers
}

// MOCKS

type mockDialer struct {
	connection *mockConnection
	connected  chan interface{}
	dialErr    error
	totalDials int
}

func (d *mockDialer) Dial() (ConnectionInterface, error) {
	d.totalDials += 1

	time.Sleep(10 * time.Millisecond)

	d.connection = newMockConnection()

	if d.connected != nil {
		close(d.connected)
		d.connected = make(chan interface{})
	}

	return d.connection, d.dialErr
}

func newMockDialer() *mockDialer {
	return &mockDialer{
		connected: make(chan interface{}),
	}
}
