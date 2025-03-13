package s6

import (
	"context"
	"fmt"
	"log"
	"time"

	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/pkg/service/s6"
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
func (s *S6Instance) initiateS6Create(ctx context.Context) error {
	log.Printf("[S6Instance] Starting Action: Creating S6 service %s ...", s.baseFSMInstance.GetID())

	// Check if we have a config with command or other settings
	configEmpty := s.config.S6ServiceConfig.Command == nil && s.config.S6ServiceConfig.Env == nil && s.config.S6ServiceConfig.ConfigFiles == nil

	if !configEmpty {
		// Create service with custom configuration
		err := s.service.Create(ctx, s.servicePath, s.config.S6ServiceConfig)
		if err != nil {
			return fmt.Errorf("failed to create service with config for %s: %w", s.baseFSMInstance.GetID(), err)
		}
	} else {
		// Simple creation with no configuration, useful for testing
		err := s.service.Create(ctx, s.servicePath, s6service.S6ServiceConfig{})
		if err != nil {
			return fmt.Errorf("failed to create service directory for %s: %w", s.baseFSMInstance.GetID(), err)
		}
	}

	log.Printf("[S6Instance] S6 service %s directory structure created", s.baseFSMInstance.GetID())
	return nil
}

// InitiateS6Remove attempts to remove the S6 service directory structure.
// It requires the service to be stopped before removal.
func (s *S6Instance) initiateS6Remove(ctx context.Context) error {
	log.Printf("[S6Instance] Starting Action: Removing S6 service %s ...", s.baseFSMInstance.GetID())

	// First ensure the service is stopped
	if s.IsS6Running() {
		return fmt.Errorf("service %s cannot be removed while running", s.baseFSMInstance.GetID())
	}

	// Remove the service directory
	err := s.service.Remove(ctx, s.servicePath)
	if err != nil {
		return fmt.Errorf("failed to remove service directory for %s: %w", s.baseFSMInstance.GetID(), err)
	}

	log.Printf("[S6Instance] S6 service %s removed", s.baseFSMInstance.GetID())
	return nil
}

// InitiateS6Start attempts to start the S6 service.
func (s *S6Instance) initiateS6Start(ctx context.Context) error {
	log.Printf("[S6Instance] Starting Action: Starting S6 service %s ...", s.baseFSMInstance.GetID())

	err := s.service.Start(ctx, s.servicePath)
	if err != nil {
		return fmt.Errorf("failed to start S6 service %s: %w", s.baseFSMInstance.GetID(), err)
	}

	log.Printf("[S6Instance] S6 service %s start command executed", s.baseFSMInstance.GetID())
	return nil
}

// InitiateS6Stop attempts to stop the S6 service.
func (s *S6Instance) initiateS6Stop(ctx context.Context) error {
	log.Printf("[S6Instance] Starting Action: Stopping S6 service %s ...", s.baseFSMInstance.GetID())

	err := s.service.Stop(ctx, s.servicePath)
	if err != nil {
		return fmt.Errorf("failed to stop S6 service %s: %w", s.baseFSMInstance.GetID(), err)
	}

	log.Printf("[S6Instance] S6 service %s stop command executed", s.baseFSMInstance.GetID())
	return nil
}

// InitiateS6Restart attempts to restart the S6 service.
func (s *S6Instance) initiateS6Restart(ctx context.Context) error {
	log.Printf("[S6Instance] Starting Action: Restarting S6 service %s ...", s.baseFSMInstance.GetID())

	err := s.service.Restart(ctx, s.servicePath)
	if err != nil {
		return fmt.Errorf("failed to restart S6 service %s: %w", s.baseFSMInstance.GetID(), err)
	}

	log.Printf("[S6Instance] S6 service %s restart command executed", s.baseFSMInstance.GetID())
	return nil
}

// UpdateObservedState updates the observed state of the service
func (s *S6Instance) updateObservedState(ctx context.Context) error {
	info, err := s.service.Status(ctx, s.servicePath)
	if err != nil {
		s.ObservedState.ServiceInfo.Status = s6service.ServiceUnknown

		return err
	}

	// Store the raw service info
	s.ObservedState.ServiceInfo = info

	// Map the service status to FSM status
	switch info.Status {
	case s6service.ServiceUp:
		s.ObservedState.ServiceInfo.Status = s6service.ServiceUp
	case s6service.ServiceDown:
		s.ObservedState.ServiceInfo.Status = s6service.ServiceDown
	case s6service.ServiceRestarting:
		s.ObservedState.ServiceInfo.Status = s6service.ServiceRestarting
	case s6service.ServiceFailed:
		s.ObservedState.ServiceInfo.Status = s6service.ServiceFailed
	default:
		s.ObservedState.ServiceInfo.Status = s6service.ServiceUnknown
	}

	// Set LastStateChange time if this is the first update
	if s.ObservedState.LastStateChange == 0 {
		s.ObservedState.LastStateChange = time.Now().Unix()
	}

	// Fetch the actual service config from s6
	config, err := s.service.GetConfig(ctx, s.servicePath)
	if err != nil {
		return fmt.Errorf("failed to get S6 service config for %s: %w", s.baseFSMInstance.GetID(), err)
	}
	s.ObservedState.ObservedS6ServiceConfig = config

	// TODO: trigger a reconcile if observed config is different from desired config

	return nil
}

// IsS6Running checks if the S6 service is running.
func (s *S6Instance) IsS6Running() bool {
	return s.ObservedState.ServiceInfo.Status == s6service.ServiceUp
}

// IsS6Stopped checks if the S6 service is stopped.
func (s *S6Instance) IsS6Stopped() bool {
	return s.ObservedState.ServiceInfo.Status == s6service.ServiceDown
}

// HasS6Failed checks if the S6 service has failed.
func (s *S6Instance) HasS6Failed() bool {
	return s.ObservedState.ServiceInfo.Status == s6service.ServiceFailed
}

// GetServicePid gets the process ID of the running service.
// Returns -1 if the service is not running.
func (s *S6Instance) GetServicePid() int {
	if s.IsS6Running() {
		return s.ObservedState.ServiceInfo.Pid
	}
	return -1
}

// GetServiceUptime gets the uptime of the service in seconds.
// Returns -1 if the service is not running.
func (s *S6Instance) GetServiceUptime() int64 {
	if s.IsS6Running() {
		return s.ObservedState.ServiceInfo.Uptime
	}
	return -1
}

// GetExitCode gets the last exit code of the service.
// Returns -1 if the service is running.
func (s *S6Instance) GetExitCode() int {
	if s.IsS6Running() {
		return -1
	}
	return s.ObservedState.ServiceInfo.ExitCode
}

// IsServiceWantingUp checks if the service is attempting to start.
func (s *S6Instance) IsServiceWantingUp() bool {
	return s.ObservedState.ServiceInfo.WantUp
}

// GetExitHistory gets the history of service exit events.
func (s *S6Instance) GetExitHistory() []s6service.ExitEvent {
	return s.ObservedState.ServiceInfo.ExitHistory
}
