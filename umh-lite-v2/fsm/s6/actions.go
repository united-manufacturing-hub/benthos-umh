package s6

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
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

	// Create service directory if it doesn't exist
	if err := os.MkdirAll(s.ServicePath, 0755); err != nil {
		return fmt.Errorf("failed to create service directory for %s: %w", s.ID, err)
	}

	// Create run script directory
	runDir := filepath.Join(s.ServicePath, "run")
	if err := os.MkdirAll(filepath.Dir(runDir), 0755); err != nil {
		return fmt.Errorf("failed to create run directory for %s: %w", s.ID, err)
	}

	// Create down file to prevent automatic startup
	downFilePath := filepath.Join(s.ServicePath, "down")
	if _, err := os.Stat(downFilePath); os.IsNotExist(err) {
		if _, err := os.Create(downFilePath); err != nil {
			return fmt.Errorf("failed to create down file for %s: %w", s.ID, err)
		}
	}

	log.Printf("S6 service %s directory structure created", s.ID)
	return nil
}

// InitiateS6Remove attempts to remove the S6 service directory structure.
func (s *S6Instance) InitiateS6Remove(ctx context.Context) error {
	log.Printf("Starting Action: Removing S6 service %s ...", s.ID)

	// First ensure the service is stopped
	if s.IsS6Running() {
		if err := s.InitiateS6Stop(ctx); err != nil {
			return fmt.Errorf("failed to stop service %s before removal: %w", s.ID, err)
		}

		// Wait for the service to stop
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if s.IsS6Stopped() {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		if !s.IsS6Stopped() {
			return fmt.Errorf("service %s did not stop in time before removal", s.ID)
		}
	}

	// Remove the service directory
	if err := os.RemoveAll(s.ServicePath); err != nil {
		return fmt.Errorf("failed to remove service directory for %s: %w", s.ID, err)
	}

	log.Printf("S6 service %s removed", s.ID)
	return nil
}

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

// UpdateExternalState updates the external state of the service
func (s *S6Instance) UpdateExternalState(ctx context.Context) error {
	status, err := s.getS6Status(ctx)
	if err != nil {
		s.ExternalState.Status = S6ServiceUnknown
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.ExternalState.Status = status

	// Try to get additional information using s6-svdt
	cmd := exec.CommandContext(ctx, "s6-svdt", s.ServicePath)
	output, err := cmd.CombinedOutput()
	if err == nil {
		// Parse output to get more details
		// Example: uptime=123, pid=1234, last change=timestamp
		outputStr := string(output)

		// Parse uptime
		if uptimeIndex := strings.Index(outputStr, "uptime="); uptimeIndex >= 0 {
			endIndex := strings.Index(outputStr[uptimeIndex:], " ")
			if endIndex > 0 {
				uptime := outputStr[uptimeIndex+7 : uptimeIndex+endIndex]
				s.ExternalState.Uptime, _ = parseUptime(uptime)
			}
		}

		// Parse PID
		if pidIndex := strings.Index(outputStr, "pid="); pidIndex >= 0 {
			endIndex := strings.Index(outputStr[pidIndex:], " ")
			if endIndex > 0 {
				pidStr := outputStr[pidIndex+4 : pidIndex+endIndex]
				s.ExternalState.Pid, _ = parsePid(pidStr)
			}
		}
	}

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
	// Check if service directory exists
	if _, err := os.Stat(s.ServicePath); os.IsNotExist(err) {
		return S6ServiceUnknown, nil
	}

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

// Helper functions for parsing the output of s6-svdt
func parseUptime(uptime string) (int64, error) {
	var seconds int64
	_, err := fmt.Sscanf(uptime, "%d", &seconds)
	return seconds, err
}

func parsePid(pid string) (int, error) {
	var pidInt int
	_, err := fmt.Sscanf(pid, "%d", &pidInt)
	return pidInt, err
}
