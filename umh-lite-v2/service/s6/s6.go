package s6

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// ServiceStatus represents the status of an S6 service
type ServiceStatus string

const (
	// ServiceUnknown indicates the service status cannot be determined
	ServiceUnknown ServiceStatus = "unknown"
	// ServiceUp indicates the service is running
	ServiceUp ServiceStatus = "up"
	// ServiceDown indicates the service is stopped
	ServiceDown ServiceStatus = "down"
	// ServiceRestarting indicates the service is restarting
	ServiceRestarting ServiceStatus = "restarting"
	// ServiceFailed indicates the service has failed
	ServiceFailed ServiceStatus = "failed"
)

// ServiceInfo contains information about an S6 service
type ServiceInfo struct {
	Status ServiceStatus
	Uptime int64
	Pid    int
}

// Service defines the interface for interacting with S6 services
type Service interface {
	// Create creates the service directory structure
	Create(ctx context.Context, servicePath string) error
	// Remove removes the service directory structure
	Remove(ctx context.Context, servicePath string) error
	// Start starts the service
	Start(ctx context.Context, servicePath string) error
	// Stop stops the service
	Stop(ctx context.Context, servicePath string) error
	// Restart restarts the service
	Restart(ctx context.Context, servicePath string) error
	// Status gets the current status of the service
	Status(ctx context.Context, servicePath string) (ServiceInfo, error)
	// ServiceExists checks if the service directory exists
	ServiceExists(servicePath string) bool
}

// DefaultService is the default implementation of the S6 Service interface
type DefaultService struct{}

// NewDefaultService creates a new default S6 service
func NewDefaultService() Service {
	return &DefaultService{}
}

// Create creates the S6 service directory structure
func (s *DefaultService) Create(ctx context.Context, servicePath string) error {
	// Create service directory if it doesn't exist
	if err := os.MkdirAll(servicePath, 0755); err != nil {
		return fmt.Errorf("failed to create service directory: %w", err)
	}

	// Create down file to prevent automatic startup
	downFilePath := servicePath + "/down"
	if _, err := os.Stat(downFilePath); os.IsNotExist(err) {
		if _, err := os.Create(downFilePath); err != nil {
			return fmt.Errorf("failed to create down file: %w", err)
		}
	}

	return nil
}

// Remove removes the S6 service directory structure
func (s *DefaultService) Remove(ctx context.Context, servicePath string) error {
	return os.RemoveAll(servicePath)
}

// Start starts the S6 service
func (s *DefaultService) Start(ctx context.Context, servicePath string) error {
	cmd := exec.CommandContext(ctx, "s6-svc", "-u", servicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to start service: %w, output: %s", err, string(output))
	}
	return nil
}

// Stop stops the S6 service
func (s *DefaultService) Stop(ctx context.Context, servicePath string) error {
	cmd := exec.CommandContext(ctx, "s6-svc", "-d", servicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to stop service: %w, output: %s", err, string(output))
	}
	return nil
}

// Restart restarts the S6 service
func (s *DefaultService) Restart(ctx context.Context, servicePath string) error {
	cmd := exec.CommandContext(ctx, "s6-svc", "-r", servicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to restart service: %w, output: %s", err, string(output))
	}
	return nil
}

// Status gets the current status of the S6 service
func (s *DefaultService) Status(ctx context.Context, servicePath string) (ServiceInfo, error) {
	info := ServiceInfo{
		Status: ServiceUnknown,
	}

	// Check if service directory exists
	if !s.ServiceExists(servicePath) {
		return info, nil
	}

	cmd := exec.CommandContext(ctx, "s6-svstat", servicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return info, fmt.Errorf("failed to get status: %w, output: %s", err, string(output))
	}

	outputStr := string(output)

	// Parse the output from s6-svstat
	if strings.Contains(outputStr, "up") {
		info.Status = ServiceUp
	} else if strings.Contains(outputStr, "down") {
		info.Status = ServiceDown
	} else if strings.Contains(outputStr, "restarting") {
		info.Status = ServiceRestarting
	} else {
		info.Status = ServiceFailed
	}

	// Try to get additional information using s6-svdt
	detailCmd := exec.CommandContext(ctx, "s6-svdt", servicePath)
	detailOutput, err := detailCmd.CombinedOutput()
	if err == nil {
		// Parse output to get more details
		detailStr := string(detailOutput)

		// Parse uptime
		if uptimeIndex := strings.Index(detailStr, "uptime="); uptimeIndex >= 0 {
			endIndex := strings.Index(detailStr[uptimeIndex:], " ")
			if endIndex > 0 {
				uptime := detailStr[uptimeIndex+7 : uptimeIndex+endIndex]
				info.Uptime, _ = parseUptime(uptime)
			}
		}

		// Parse PID
		if pidIndex := strings.Index(detailStr, "pid="); pidIndex >= 0 {
			endIndex := strings.Index(detailStr[pidIndex:], " ")
			if endIndex > 0 {
				pidStr := detailStr[pidIndex+4 : pidIndex+endIndex]
				info.Pid, _ = parsePid(pidStr)
			}
		}
	}

	return info, nil
}

// ServiceExists checks if the service directory exists
func (s *DefaultService) ServiceExists(servicePath string) bool {
	_, err := os.Stat(servicePath)
	return !os.IsNotExist(err)
}

// Helper functions for parsing the output
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
