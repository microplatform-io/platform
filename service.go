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

	"github.com/sirupsen/logrus"
)

type Handler interface {
	ServePlatform(responder Responder, request *Request)
}

type HandlerFunc func(responder Responder, request *Request)

func (handlerFunc HandlerFunc) ServePlatform(responder Responder, request *Request) {
	handlerFunc(responder, request)
}

func capturePanic(fn func(r interface{})) {
	if !PREVENT_PLATFORM_PANICS {
		return
	}

	if r := recover(); r != nil {
		fn(r)
	}
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
	name       string
	publisher  Publisher
	subscriber Subscriber
	tracer     Tracer
	responder  Responder

	healthManager  HealthManager
	healthCheckers []HealthChecker

	mu                sync.Mutex
	closed            bool
	workerPendingJobs int32
	workerQuitChan    chan interface{}
	allWorkersDone    chan interface{}
}

func (s *Service) generateResponder(request *Request, path string) Responder {
	responder := s.responder

	if s.tracer != nil {
		if request.Trace == nil {
			request.Trace = s.tracer.Start(nil, path)
		}

		responder = NewTraceResponder(s.responder, s.tracer)
	}

	return NewRequestResponder(responder, request)
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

		responder := s.generateResponder(request, path)

		defer capturePanic(func(r interface{}) {
			logger.WithFields(logrus.Fields{
				"resource_type": "handler",
				"path":          path,
				"reason":        r,
			}).Error("Service has panicked!")

			panicErrorBytes, _ := Marshal(&Error{
				Message: String(fmt.Sprintf("A fatal error has occurred. %s: %s %s", path, identifyPanic(), r)),
			})

			responder.Respond(&Request{
				Routing:   RouteToUri("resource:///platform/reply/error"),
				Payload:   panicErrorBytes,
				Completed: Bool(true),
			})

			s.publisher.Publish("panic.handler."+path, body)
		})

		handler.ServePlatform(responder, request)

		return nil
	}))
}

func (s *Service) AddHealthChecker(healthChecker HealthChecker) {
	s.healthCheckers = append(s.healthCheckers, healthChecker)
}

func (s *Service) AddListener(topic string, handler ConsumerHandler) {
	logger.Infoln("[Service.AddListener] Adding listener", topic)

	s.subscriber.Subscribe(topic, ConsumerHandlerFunc(func(body []byte) error {
		s.incrementWorkerPendingJobs()
		defer s.decrementWorkerPendingJobs()

		defer capturePanic(func(r interface{}) {
			logger.WithFields(logrus.Fields{
				"resource_type": "listener",
				"topic":         topic,
				"reason":        r,
			}).Error("Service has panicked!")

			s.publisher.Publish("panic.listener."+topic, body)
		})

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

	go func() {
		for {
			select {
			case err := <-s.checkHealth():
				if err != nil {
					logger.WithError(err).Error("Service has failed a health check")

					s.healthManager.SetHealthStatus(HealthStatus{
						IsHealthy:   false,
						Description: err.Error(),
					})
				} else {
					s.healthManager.SetHealthStatus(HealthStatus{
						IsHealthy: true,
					})
				}
			case <-time.After(10 * time.Second):
				logger.Error("Service health check has timed out")

				s.healthManager.SetHealthStatus(HealthStatus{
					IsHealthy:   false,
					Description: "Health check exceeded time limit of 10 seconds",
				})
			}

			time.Sleep(30 * time.Second)
		}
	}()

	// Close the service if we detect a kill signal
	go func() {
		select {
		case <-sigc:
			s.Close()
		}
	}()

	go s.healthManager.Run()

	s.subscriber.Run()

	<-s.allWorkersDone
}

func (s *Service) checkHealth() <-chan error {
	errChan := make(chan error)

	go func() {
		var healthErr error

		for i := range s.healthCheckers {
			if err := s.healthCheckers[i].CheckHealth(); err != nil {
				healthErr = err
				break
			}
		}

		errChan <- healthErr
		close(errChan)
	}()

	return errChan
}

func NewService(serviceName string, publisher Publisher, subscriber Subscriber, tracer Tracer) (*Service, error) {
	return &Service{
		name:       serviceName,
		tracer:     tracer,
		subscriber: subscriber,
		publisher:  publisher,
		responder:  NewPublishResponder(publisher),

		healthManager: NewFileHealthManager("/tmp/healthy"),
		healthCheckers: []HealthChecker{
			newPlatformHealthChecker(publisher),
		},

		workerQuitChan: make(chan interface{}),
		allWorkersDone: make(chan interface{}),
	}, nil
}

func NewServiceWithResponder(serviceName string, publisher Publisher, subscriber Subscriber, tracer Tracer, responder Responder) (*Service, error) {
	return &Service{
		name:       serviceName,
		tracer:     tracer,
		subscriber: subscriber,
		publisher:  publisher,
		responder:  responder,

		healthManager: NewFileHealthManager("/tmp/healthy"),
		healthCheckers: []HealthChecker{
			newPlatformHealthChecker(publisher),
		},

		workerQuitChan: make(chan interface{}),
		allWorkersDone: make(chan interface{}),
	}, nil
}
