package platform

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Handler interface {
	Handle(responder Responder, request *Request)
}

type HandlerFunc func(responder Responder, request *Request)

func (handlerFunc HandlerFunc) Handle(responder Responder, request *Request) {
	handlerFunc(responder, request)
}

func identifyPanic() string {
	var name, file string
	var line int
	var pc [16]uintptr

	n := runtime.Callers(3, pc[:])
	for _, pc := range pc[:n] {
		fn := runtime.FuncForPC(pc)
		if fn == nil {
			continue
		}
		file, line = fn.FileLine(pc)
		name = fn.Name()
		if !strings.HasPrefix(name, "runtime.") {
			break
		}
	}

	switch {
	case name != "":
		return fmt.Sprintf("%v:%v", name, line)
	case file != "":
		return fmt.Sprintf("%v:%v", file, line)
	}

	return fmt.Sprintf("pc:%x", pc)
}

type Service struct {
	publisher  Publisher
	subscriber Subscriber
	tracer     Tracer
	responder  Responder
	name       string

	mu                sync.Mutex
	closed            bool
	workerPendingJobs int32
	workerQuitChan    chan interface{}
	allWorkersDone    chan interface{}
}

func (s *Service) AddHandler(path string, handler Handler) {
	logger.Infoln("[Service.AddHandler] adding handler", path)

	s.subscriber.Subscribe("microservice-"+path, ConsumerHandlerFunc(func(body []byte) error {
		// TODO: This error is ignored at the subscriber level, so therefore this message is permanently lost!
		if !s.canAcceptWork() {
			return errors.New("no new work can be accepted")
		}

		s.incrementWorkerPendingJobs()
		defer s.decrementWorkerPendingJobs()

		request := &Request{}
		if err := Unmarshal(body, request); err != nil {
			return nil
		}

		var responder Responder

		if s.tracer != nil {
			if request.Trace == nil {
				request.Trace = s.tracer.Start(request.Trace, path)
			}

			traceResponder := NewTraceResponder(s.responder, s.tracer)
			responder = NewRequestResponder(traceResponder, request)
		} else {
			responder = NewRequestResponder(s.responder, request)
		}

		if PREVENT_PLATFORM_PANICS {
			defer func() {
				if r := recover(); r != nil {
					panicErrorBytes, _ := Marshal(&Error{
						Message: String(fmt.Sprintf("A fatal error has occurred. %s: %s %s", path, identifyPanic(), r)),
					})

					responder.Respond(&Request{
						Routing:   RouteToUri("resource:///platform/reply/error"),
						Payload:   panicErrorBytes,
						Completed: Bool(true),
					})

					s.publisher.Publish("panic.handler."+path, body)
				}
			}()
		}

		handler.Handle(responder, request)

		return nil
	}))
}

func (s *Service) AddListener(topic string, handler ConsumerHandler) {
	logger.Infoln("[Service.AddListener] Adding listener", topic)
	s.subscriber.Subscribe(topic, ConsumerHandlerFunc(func(body []byte) error {
		s.incrementWorkerPendingJobs()
		defer s.decrementWorkerPendingJobs()

		if PREVENT_PLATFORM_PANICS {
			defer func() {
				if r := recover(); r != nil {
					s.publisher.Publish("panic.listener."+topic, body)
				}
			}()
		}

		logger.Infof("[Service.AddListener] Handling %s publish", topic)

		return handler.HandleMessage(body)
	}))
}

func (s *Service) canAcceptWork() bool {
	select {
	case <-s.workerQuitChan:
		return false
	default:
		return true
	}
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.closed {
		logger.Infoln("[Service.Close] service is shutting down")

		s.closed = true

		close(s.workerQuitChan)

		logger.Infof("[Service.Close] pending jobs: %d", s.workerPendingJobs)

		if s.workerPendingJobs > 0 {
			<-s.allWorkersDone
		} else {
			close(s.allWorkersDone)
		}

		logger.Infoln("[Service.Close] all workers have finished")
	} else {
		logger.Infoln("[Service.Close] service has already been shut down")
	}

	// Something about this helps graceful shut downs, let's look into it more
	time.Sleep(1 * time.Second)

	return nil
}

func (s *Service) incrementWorkerPendingJobs() {
	atomic.AddInt32(&s.workerPendingJobs, 1)
}

func (s *Service) decrementWorkerPendingJobs() {
	atomic.AddInt32(&s.workerPendingJobs, -1)

	if s.closed && s.workerPendingJobs == 0 {
		close(s.allWorkersDone)
	}
}

func (s *Service) Run() {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)

	// Close the service if we detect a kill signal
	go func() {
		select {
		case <-sigc:
			s.Close()
		}
	}()

	s.subscriber.Run()

	<-s.allWorkersDone
}

func NewService(serviceName string, publisher Publisher, subscriber Subscriber, tracer Tracer) (*Service, error) {
	return &Service{
		tracer:         tracer,
		subscriber:     subscriber,
		publisher:      publisher,
		responder:      NewPublishResponder(publisher),
		name:           serviceName,
		workerQuitChan: make(chan interface{}),
		allWorkersDone: make(chan interface{}),
	}, nil
}

func NewServiceWithResponder(serviceName string, publisher Publisher, subscriber Subscriber, tracer Tracer, responder Responder) (*Service, error) {
	return &Service{
		tracer:         tracer,
		subscriber:     subscriber,
		publisher:      publisher,
		responder:      responder,
		name:           serviceName,
		workerQuitChan: make(chan interface{}),
		allWorkersDone: make(chan interface{}),
	}, nil
}
