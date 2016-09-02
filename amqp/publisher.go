package amqp

import (
	"sync"

	"github.com/microplatform-io/platform"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

const MAX_PUBLISH_RETRIES = 3

type Publisher struct {
	dialerInterface     DialerInterface
	connectionInterface ConnectionInterface
	channelInterface    ChannelInterface
	mu                  sync.Mutex
}

func (p *Publisher) getChannelInterface() ChannelInterface {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.channelInterface
}

func (p *Publisher) keepAlive() {
	connectionClosed := p.connectionInterface.NotifyClose(make(chan *amqp.Error))
	channelClosed := p.channelInterface.NotifyClose(make(chan *amqp.Error))

	for {
		select {
		case <-connectionClosed:
			p.mu.Lock()
			connectionClosed = p.resetConnection()
			channelClosed = p.resetChannel()
			p.mu.Unlock()

		case <-channelClosed:
			p.mu.Lock()
			channelClosed = p.resetChannel()
			p.mu.Unlock()
		}
	}
}

func (p *Publisher) resetConnection() chan *amqp.Error {
	connectionClosed := make(chan *amqp.Error)

	connectionInterface, err := p.dialerInterface.Dial()
	if err != nil {
		close(connectionClosed)
	} else {
		p.connectionInterface = connectionInterface
		p.connectionInterface.NotifyClose(connectionClosed)
	}

	return connectionClosed
}

func (p *Publisher) resetChannel() chan *amqp.Error {
	channelClosed := p.channelInterface.NotifyClose(make(chan *amqp.Error))

	channelInterface, err := p.connectionInterface.GetChannelInterface()
	if err != nil {
		close(channelClosed)
	} else {
		p.channelInterface = channelInterface
		p.channelInterface.NotifyClose(channelClosed)
	}

	return channelClosed
}

func (p *Publisher) Publish(topic string, body []byte) error {
	var publishErr error

	for i := 0; i < MAX_PUBLISH_RETRIES; i++ {
		publishErr = p.getChannelInterface().Publish(
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
	}

	return publishErr
}

func NewPublisher(dialerInterface DialerInterface) (*Publisher, error) {
	connectionInterface, err := dialerInterface.Dial()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create a connection interface within the publisher")
	}

	channelInterface, err := connectionInterface.GetChannelInterface()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create a channel interface within the publisher")
	}

	publisher := &Publisher{
		dialerInterface:     dialerInterface,
		connectionInterface: connectionInterface,
		channelInterface:    channelInterface,
	}

	go publisher.keepAlive()

	return publisher, nil
}

func NewMultiPublisher(dialerInterfaces []DialerInterface) (platform.Publisher, error) {
	publishers := make([]platform.Publisher, len(dialerInterfaces))

	for i := range dialerInterfaces {
		publisher, err := NewPublisher(dialerInterfaces[i])
		if err != nil {
			return nil, err
		}

		publishers[i] = publisher
	}

	return platform.NewMultiPublisher(publishers), nil
}
