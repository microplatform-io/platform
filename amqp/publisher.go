package amqp

import (
	"sync"

	"github.com/microplatform-io/platform"
	"github.com/streadway/amqp"
)

const MAX_PUBLISH_RETRIES = 3

type Publisher struct {
	dialerInterface  DialerInterface
	channelInterface ChannelInterface
	mu               sync.Mutex
}

func (p *Publisher) getChannel() (ChannelInterface, error) {
	if p.channelInterface != nil {
		return p.channelInterface, nil
	}

	if err := p.resetChannel(); err != nil {
		return nil, err
	}

	return p.channelInterface, nil
}

func (p *Publisher) resetChannel() error {
	connection, err := p.dialerInterface.Dial()
	if err != nil {
		return err
	}

	channelInterface, err := connection.GetChannelInterface()
	if err != nil {
		return err
	}

	p.mu.Lock()
	p.channelInterface = channelInterface
	p.mu.Unlock()

	return nil
}

func (p *Publisher) Publish(topic string, body []byte) error {
	var publishErr error

	for i := 0; i < MAX_PUBLISH_RETRIES; i++ {
		logger.Printf("[Publisher.Publish] publishing for %s attempt %d of %d", topic, i+1, MAX_PUBLISH_RETRIES)

		channelInterface, err := p.getChannel()
		if err != nil {
			logger.Printf("[Publisher.Publish] failed to get channelInterface: %s", err)
			publishErr = err
			continue
		}

		publishErr = channelInterface.Publish(
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
			logger.Printf("[Publisher.Publish] published for %s", topic)
			return nil
		}

		logger.Printf("[Publisher.Publish] error publishing for %s - %s", topic, publishErr)

		if err := p.resetChannel(); err != nil {
			logger.Printf("[Publisher.Publish] failed to reset channelInterface: %s", err)
			continue
		}
	}

	return publishErr
}

func NewPublisher(dialerInterface DialerInterface) (*Publisher, error) {
	return &Publisher{
		dialerInterface: dialerInterface,
	}, nil
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
