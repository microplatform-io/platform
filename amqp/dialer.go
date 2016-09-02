package amqp

import (
	"sync"
	"time"

	"github.com/cenk/backoff"
	"github.com/pkg/errors"
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

	if d.connection == nil {
		return nil, errors.New("connection is not available")
	}

	return d.connection, nil
}

func (d *CachingDialer) keepAlive() {
	for {
		d.mu.Lock()
		closed := d.connection.NotifyClose(make(chan *amqp.Error))
		d.mu.Unlock()

		<-closed

		d.mu.Lock()
		err := backoff.Retry(func() error {
			connection, err := d.dialer.Dial()
			if err != nil {
				return err
			}

			d.connection = connection

			return nil
		}, backoff.NewExponentialBackOff())
		if err != nil {
			panic(err)
		}
		d.mu.Unlock()
	}
}

func NewCachingDialer(url string) *CachingDialer {
	dialer := NewDialer(url)

	connection, err := dialer.Dial()
	if err != nil {
		panic(err)
	}

	cachingDialer := &CachingDialer{
		dialer:     dialer,
		connection: connection,
	}

	go cachingDialer.keepAlive()

	return cachingDialer
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
