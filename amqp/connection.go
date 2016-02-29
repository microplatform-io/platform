package amqp

import (
	"errors"

	"github.com/streadway/amqp"
)

type ConnectionInterface interface {
	Close() error
	GetChannelInterface() (ChannelInterface, error)
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
}

type Connection struct {
	connection *amqp.Connection
}

func (c *Connection) Close() error {
	return c.connection.Close()
}

func (c *Connection) GetChannelInterface() (ChannelInterface, error) {
	channel, err := c.connection.Channel()
	if err != nil {
		return nil, err
	}

	return &Channel{
		channel: channel,
	}, nil
}

func (c *Connection) NotifyClose(ch chan *amqp.Error) chan *amqp.Error {
	return c.connection.NotifyClose(ch)
}

// MOCKS

type mockConnection struct {
	channel        *mockChannel
	closeNotifiers []chan *amqp.Error
	closed         bool
}

func (c *mockConnection) Close() error {
	c.channel.Close()

	for i := range c.closeNotifiers {
		close(c.closeNotifiers[i])
	}

	c.closeNotifiers = []chan *amqp.Error{}
	c.closed = true

	return nil
}

func (c *mockConnection) GetChannelInterface() (ChannelInterface, error) {
	if c.closed {
		return nil, errors.New("mock connector has been closed")
	}

	return c.channel, nil
}

func (c *mockConnection) NotifyClose(ch chan *amqp.Error) chan *amqp.Error {
	c.closeNotifiers = append(c.closeNotifiers, ch)

	return ch
}

func newMockConnection() *mockConnection {
	return &mockConnection{
		channel: newMockChannel(),
	}
}
