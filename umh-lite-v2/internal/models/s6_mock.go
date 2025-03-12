package models

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// S6ServiceStatus represents the state of a service in s6-overlay
type S6ServiceStatus string

const (
	// S6ServiceDown indicates the service is not running
	S6ServiceDown S6ServiceStatus = "down"
	// S6ServiceUp indicates the service is running
	S6ServiceUp S6ServiceStatus = "up"
	// S6ServiceRestarting indicates the service is restarting
	S6ServiceRestarting S6ServiceStatus = "restarting"
	// S6ServiceFailed indicates the service has failed
	S6ServiceFailed S6ServiceStatus = "failed"
)

// S6ServiceMock simulates the behavior of an s6-supervised service
type S6ServiceMock struct {
	// ServiceName is the name of the service
	ServiceName string

	// mu protects concurrent access to service state
	mu sync.RWMutex

	// Status is the current status of the service
	Status S6ServiceStatus

	// ExitCode is the exit code of the service if it's down
	ExitCode int

	// StartTime is when the service was started
	StartTime time.Time

	// StopTime is when the service was stopped
	StopTime time.Time

	// StartCount is how many times the service has been started
	StartCount int

	// StopCount is how many times the service has been stopped
	StopCount int

	// FailCount is how many times the service has failed
	FailCount int

	// IsReady indicates if the service is ready to accept connections
	IsReady bool

	// StartDelay simulates the time it takes for the service to start
	StartDelay time.Duration

	// StopDelay simulates the time it takes for the service to stop
	StopDelay time.Duration

	// ShouldFailOnStart indicates if the service should fail when started
	ShouldFailOnStart bool

	// ShouldFailOnStop indicates if the service should fail when stopped
	ShouldFailOnStop bool
}

// NewS6ServiceMock creates a new mock S6 service
func NewS6ServiceMock(name string) *S6ServiceMock {
	return &S6ServiceMock{
		ServiceName: name,
		Status:      S6ServiceDown,
		StartDelay:  100 * time.Millisecond,
		StopDelay:   100 * time.Millisecond,
	}
}

// Start simulates starting the service
func (s *S6ServiceMock) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.StartCount++

	if s.Status == S6ServiceUp {
		return fmt.Errorf("service %s is already running", s.ServiceName)
	}

	s.Status = S6ServiceRestarting

	// Simulate startup delay
	select {
	case <-time.After(s.StartDelay):
		// Continue with start
	case <-ctx.Done():
		s.Status = S6ServiceDown
		return fmt.Errorf("context canceled while starting service %s: %w", s.ServiceName, ctx.Err())
	}

	if s.ShouldFailOnStart {
		s.Status = S6ServiceFailed
		s.FailCount++
		s.ExitCode = 1
		return fmt.Errorf("service %s failed to start", s.ServiceName)
	}

	s.Status = S6ServiceUp
	s.StartTime = time.Now()
	s.IsReady = true

	return nil
}

// Stop simulates stopping the service
func (s *S6ServiceMock) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.StopCount++

	if s.Status == S6ServiceDown {
		return fmt.Errorf("service %s is already stopped", s.ServiceName)
	}

	// Simulate stop delay
	select {
	case <-time.After(s.StopDelay):
		// Continue with stop
	case <-ctx.Done():
		return fmt.Errorf("context canceled while stopping service %s: %w", s.ServiceName, ctx.Err())
	}

	if s.ShouldFailOnStop {
		return fmt.Errorf("service %s failed to stop", s.ServiceName)
	}

	s.Status = S6ServiceDown
	s.StopTime = time.Now()
	s.IsReady = false

	return nil
}

// GetStatus returns the current status of the service
func (s *S6ServiceMock) GetStatus() S6ServiceStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Status
}

// IsRunning returns true if the service is running
func (s *S6ServiceMock) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Status == S6ServiceUp
}

// SimulateFailure causes the service to fail
func (s *S6ServiceMock) SimulateFailure(exitCode int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Status = S6ServiceFailed
	s.ExitCode = exitCode
	s.FailCount++
	s.IsReady = false
}

// SimulateReady marks the service as ready
func (s *S6ServiceMock) SimulateReady() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.IsReady = true
}

// GetMetrics returns metrics for the service
func (s *S6ServiceMock) GetMetrics() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	uptime := time.Duration(0)
	if s.Status == S6ServiceUp && !s.StartTime.IsZero() {
		uptime = time.Since(s.StartTime)
	}

	return map[string]interface{}{
		"status":      string(s.Status),
		"exit_code":   s.ExitCode,
		"start_count": s.StartCount,
		"stop_count":  s.StopCount,
		"fail_count":  s.FailCount,
		"uptime_ms":   uptime.Milliseconds(),
		"is_ready":    s.IsReady,
	}
}
