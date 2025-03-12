package s6

import (
	"context"
	"fmt"
	"log"

	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/service/s6"
)

// The functions in this file define heavier, possibly fail-prone operations
// (for example, network or file I/O) that the S6 FSM might need to perform.
// They are intended to be called from Reconcile.
//
// IMPORTANT:
//   - Each action is expected to be idempotent, since it may be retried
//     multiple times due to transient failures.
//   - Each action takes a context.Context and can return an error if the operation fails.
//   - If an error occurs, the Reconcile function must handle
//     setting S6Instance.lastError and scheduling a retry/backoff.

// InitiateS6Create attempts to create the S6 service directory structure.
func (s *S6Instance) InitiateS6Create(ctx context.Context) error {
	log.Printf("Starting Action: Creating S6 service %s ...", s.ID)

	err := s.service.Create(ctx, s.ServicePath)
	if err != nil {
		return fmt.Errorf("failed to create service directory for %s: %w", s.ID, err)
	}

	log.Printf("S6 service %s directory structure created", s.ID)
	return nil
}

// InitiateS6Remove attempts to remove the S6 service directory structure.
// It requires the service to be stopped before removal.
func (s *S6Instance) InitiateS6Remove(ctx context.Context) error {
	log.Printf("Starting Action: Removing S6 service %s ...", s.ID)

	// First ensure the service is stopped
	if s.IsS6Running() {
		return fmt.Errorf("service %s cannot be removed while running", s.ID)
	}

	// Remove the service directory
	err := s.service.Remove(ctx, s.ServicePath)
	if err != nil {
		return fmt.Errorf("failed to remove service directory for %s: %w", s.ID, err)
	}

	log.Printf("S6 service %s removed", s.ID)
	return nil
}

// InitiateS6Start attempts to start the S6 service.
func (s *S6Instance) InitiateS6Start(ctx context.Context) error {
	log.Printf("Starting Action: Starting S6 service %s ...", s.ID)

	err := s.service.Start(ctx, s.ServicePath)
	if err != nil {
		return fmt.Errorf("failed to start S6 service %s: %w", s.ID, err)
	}

	log.Printf("S6 service %s start command executed", s.ID)
	return nil
}

// InitiateS6Stop attempts to stop the S6 service.
func (s *S6Instance) InitiateS6Stop(ctx context.Context) error {
	log.Printf("Starting Action: Stopping S6 service %s ...", s.ID)

	err := s.service.Stop(ctx, s.ServicePath)
	if err != nil {
		return fmt.Errorf("failed to stop S6 service %s: %w", s.ID, err)
	}

	log.Printf("S6 service %s stop command executed", s.ID)
	return nil
}

// InitiateS6Restart attempts to restart the S6 service.
func (s *S6Instance) InitiateS6Restart(ctx context.Context) error {
	log.Printf("Starting Action: Restarting S6 service %s ...", s.ID)

	err := s.service.Restart(ctx, s.ServicePath)
	if err != nil {
		return fmt.Errorf("failed to restart S6 service %s: %w", s.ID, err)
	}

	log.Printf("S6 service %s restart command executed", s.ID)
	return nil
}

// UpdateObservedState updates the observed state of the service
func (s *S6Instance) UpdateObservedState(ctx context.Context) error {
	info, err := s.service.Status(ctx, s.ServicePath)
	if err != nil {
		s.ObservedState.Status = S6ServiceUnknown
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Map the service status to FSM status
	switch info.Status {
	case s6service.ServiceUp:
		s.ObservedState.Status = S6ServiceUp
	case s6service.ServiceDown:
		s.ObservedState.Status = S6ServiceDown
	case s6service.ServiceRestarting:
		s.ObservedState.Status = S6ServiceRestarting
	case s6service.ServiceFailed:
		s.ObservedState.Status = S6ServiceFailed
	default:
		s.ObservedState.Status = S6ServiceUnknown
	}

	s.ObservedState.Uptime = info.Uptime
	s.ObservedState.Pid = info.Pid

	return nil
}

// IsS6Running checks if the S6 service is running.
func (s *S6Instance) IsS6Running() bool {
	return s.ObservedState.Status == S6ServiceUp
}

// IsS6Stopped checks if the S6 service is stopped.
func (s *S6Instance) IsS6Stopped() bool {
	return s.ObservedState.Status == S6ServiceDown
}

// HasS6Failed checks if the S6 service has failed.
func (s *S6Instance) HasS6Failed() bool {
	return s.ObservedState.Status == S6ServiceFailed
}
