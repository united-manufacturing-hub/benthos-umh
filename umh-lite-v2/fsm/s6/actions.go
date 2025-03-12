package s6

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"strings"
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

// InitiateS6Start attempts to start the S6 service.
func (s *S6Instance) InitiateS6Start(ctx context.Context) error {
	log.Printf("Starting Action: Starting S6 service %s ...", s.ID)

	// Using s6-svc to start the service, -u means "up"
	cmd := exec.CommandContext(ctx, "s6-svc", "-u", s.ServicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to start S6 service %s: %w, output: %s", s.ID, err, string(output))
	}

	log.Printf("S6 service %s start command executed", s.ID)
	return nil
}

// InitiateS6Stop attempts to stop the S6 service.
func (s *S6Instance) InitiateS6Stop(ctx context.Context) error {
	log.Printf("Starting Action: Stopping S6 service %s ...", s.ID)

	// Using s6-svc to stop the service, -d means "down"
	cmd := exec.CommandContext(ctx, "s6-svc", "-d", s.ServicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to stop S6 service %s: %w, output: %s", s.ID, err, string(output))
	}

	log.Printf("S6 service %s stop command executed", s.ID)
	return nil
}

// InitiateS6Restart attempts to restart the S6 service.
func (s *S6Instance) InitiateS6Restart(ctx context.Context) error {
	log.Printf("Starting Action: Restarting S6 service %s ...", s.ID)

	// Using s6-svc to restart the service, -r means "restart"
	cmd := exec.CommandContext(ctx, "s6-svc", "-r", s.ServicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to restart S6 service %s: %w, output: %s", s.ID, err, string(output))
	}

	log.Printf("S6 service %s restart command executed", s.ID)
	return nil
}

// IsS6Running checks if the S6 service is running.
func (s *S6Instance) IsS6Running() bool {
	return s.ExternalState.Status == S6ServiceUp
}

// IsS6Stopped checks if the S6 service is stopped.
func (s *S6Instance) IsS6Stopped() bool {
	return s.ExternalState.Status == S6ServiceDown
}

// HasS6Failed checks if the S6 service has failed.
func (s *S6Instance) HasS6Failed() bool {
	return s.ExternalState.Status == S6ServiceFailed
}

// getS6Status retrieves the current status of the service using s6-svstat
func (s *S6Instance) getS6Status(ctx context.Context) (S6ServiceStatus, error) {
	cmd := exec.CommandContext(ctx, "s6-svstat", s.ServicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return S6ServiceUnknown, fmt.Errorf("failed to get status for S6 service %s: %w, output: %s", s.ID, err, string(output))
	}

	outputStr := string(output)

	// Parse the output from s6-svstat
	// Example output: "up (pid 1234) 123 seconds"
	if strings.Contains(outputStr, "up") {
		return S6ServiceUp, nil
	} else if strings.Contains(outputStr, "down") {
		return S6ServiceDown, nil
	} else if strings.Contains(outputStr, "restarting") {
		return S6ServiceRestarting, nil
	} else {
		// If we can't determine the status, assume it failed
		return S6ServiceFailed, nil
	}
}
