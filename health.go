package platform

import (
	"fmt"
	"github.com/pkg/errors"
	"net/http"
	"os"
)

type HealthChecker interface {
	CheckHealth() error
}

type HealthCheckerFunc func() error

func (f HealthCheckerFunc) CheckHealth() error {
	return f()
}

type platformHealthChecker struct {
	publisher Publisher
}

func (c *platformHealthChecker) CheckHealth() error {
	if err := c.publisher.Publish("healthcheck", []byte{}); err != nil {
		return errors.Wrap(err, "Failed to publish a basic healthcheck event")
	}

	return nil
}

func newPlatformHealthChecker(publisher Publisher) *platformHealthChecker {
	return &platformHealthChecker{
		publisher: publisher,
	}
}

type HealthStatus struct {
	Description string
	IsHealthy   bool
}

type HealthManager interface {
	SetHealthStatus(healthStatus HealthStatus)
	Run() error
}

type FileHealthManager struct {
	statusFile string
}

func (m *FileHealthManager) SetHealthStatus(healthStatus HealthStatus) {
	if healthStatus.IsHealthy {
		f, createErr := os.Create(m.statusFile)
		if createErr == nil {
			if _, writeErr := fmt.Fprint(f, "HEALTHY"); writeErr == nil {
				f.Close()
				return
			}
		}
	}

	os.Remove(m.statusFile)
}

func (m *FileHealthManager) Run() error {
	return nil
}

func NewFileHealthManager(statusFile string) *FileHealthManager {
	return &FileHealthManager{
		statusFile: statusFile,
	}
}

type HttpHealthManager struct {
	listenAddr   string
	healthStatus HealthStatus
}

func (m *HttpHealthManager) SetHealthStatus(healthStatus HealthStatus) {
	m.healthStatus = healthStatus
}

func (m *HttpHealthManager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m.healthStatus.IsHealthy {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, m.healthStatus.Description)
	}
}

func (m *HttpHealthManager) Run() error {
	return http.ListenAndServe(m.listenAddr, m)
}

func NewHttpHealthManager(listenAddr string) *HttpHealthManager {
	return &HttpHealthManager{
		listenAddr: listenAddr,
	}
}
