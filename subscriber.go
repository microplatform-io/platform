package platform

import "sync"

type Subscriber interface {
	Run()
	Subscribe(topic string, handler ConsumerHandler)
}

type ConsumerHandler interface {
	HandleMessage(body []byte) error
}

type ConsumerHandlerFunc func([]byte) error

func (handlerFunc ConsumerHandlerFunc) HandleMessage(p []byte) error {
	return handlerFunc(p)
}

type MultiSubscriber struct {
	subscribers []Subscriber
}

func (s *MultiSubscriber) Run() {
	if len(s.subscribers) <= 0 {
		panic("No subscribers have been declared in the multi subscriber")
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(s.subscribers))

	for i := range s.subscribers {
		go func(i int) {
			s.subscribers[i].Run()

			wg.Done()
		}(i)
	}

	wg.Wait()
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
