package platform

type MultiPublisher struct {
	publishers []Publisher

	offset int
}

func (p *MultiPublisher) Publish(topic string, body []byte) error {
	defer p.incrementOffset()

	return p.publishers[p.offset].Publish(topic, body)
}

func (p *MultiPublisher) incrementOffset() {
	p.offset = (p.offset + 1) % len(p.publishers)
}

func NewMultiPublisher(publishers []Publisher) *MultiPublisher {
	return &MultiPublisher{
		publishers: publishers,
	}
}
