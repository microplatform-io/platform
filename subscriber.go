package platform

import "sync"

type MultiSubscriber struct {
	subscribers []Subscriber
}

func (s *MultiSubscriber) Run() error {
	wg := &sync.WaitGroup{}
	wg.Add(len(s.subscribers))

	for i := range s.subscribers {
		go func(i int) {
			s.subscribers[i].Run()

			wg.Done()
		}(i)
	}

	wg.Wait()

	return nil
}

func (s *MultiSubscriber) Subscribe(topic string, handler ConsumerHandler) {
	for i := range s.subscribers {
		s.subscribers[i].Subscribe(topic, handler)
	}
}

func NewMultiSubscriber(subscribers []Subscriber) *MultiSubscriber {
	return &MultiSubscriber{
		subscribers: subscribers,
	}
}
