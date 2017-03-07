package platform

import "errors"

type Publisher interface {
	Publish(topic string, body []byte) error
	Close() error
}

type MultiPublisher struct {
	publishers []Publisher

	offset int
}

func (p *MultiPublisher) Publish(topic string, body []byte) error {
	if len(p.publishers) <= 0 {
		return errors.New("No publishers have been declared in the multi publisher")
	}

	defer p.incrementOffset()

	return p.publishers[p.offset].Publish(topic, body)
}

func (p *MultiPublisher) Close() error {
	for i := range p.publishers {
		p.publishers[i].Close()
	}

	return nil
}

func (p *MultiPublisher) incrementOffset() {
	p.offset = (p.offset + 1) % len(p.publishers)
}

func NewMultiPublisher(publishers []Publisher) *MultiPublisher {
	return &MultiPublisher{
		publishers: publishers,
	}
}
