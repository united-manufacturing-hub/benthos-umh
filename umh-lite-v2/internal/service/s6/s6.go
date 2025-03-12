package s6

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

// ServiceConfig contains configuration for creating a service
type ServiceConfig struct {
	// Command is the command to run for the service
	Command []string
	// Env is a map of environment variables for the service
	Env map[string]string
	// ConfigFiles is a map of config file paths to their contents
	ConfigFiles map[string]string
}

// Service defines the interface for interacting with S6 services
type Service interface {
	// Create creates the service with specific configuration
	Create(ctx context.Context, servicePath string, config ServiceConfig) error
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

// Create creates the S6 service with specific configuration
func (s *DefaultService) Create(ctx context.Context, servicePath string, config ServiceConfig) error {
	// Create service directory if it doesn't exist
	if err := os.MkdirAll(servicePath, 0755); err != nil {
		return fmt.Errorf("failed to create service directory: %w", err)
	}

	// Create down file to prevent automatic startup
	downFilePath := filepath.Join(servicePath, "down")
	if _, err := os.Stat(downFilePath); os.IsNotExist(err) {
		if _, err := os.Create(downFilePath); err != nil {
			return fmt.Errorf("failed to create down file: %w", err)
		}
	}

	// Create type file (required for s6-rc)
	typeFile := filepath.Join(servicePath, "type")
	if _, err := os.Stat(typeFile); os.IsNotExist(err) {
		tf, err := os.Create(typeFile)
		if err != nil {
			return fmt.Errorf("failed to create type file: %w", err)
		}
		if _, err := tf.WriteString("longrun"); err != nil {
			tf.Close()
			return fmt.Errorf("failed to write to type file: %w", err)
		}
		tf.Close()
	}

	// If command is specified, create run script
	if len(config.Command) > 0 {
		if err := s.createRunScript(servicePath, config.Command, config.Env); err != nil {
			return err
		}
	}

	// Create any config files specified
	if err := s.createConfigFiles(servicePath, config.ConfigFiles); err != nil {
		return err
	}

	// Register service in user/contents.d
	serviceName := filepath.Base(servicePath)
	userContentsDPath := filepath.Join(filepath.Dir(servicePath), "user", "contents.d")
	if err := os.MkdirAll(userContentsDPath, 0755); err != nil {
		return fmt.Errorf("failed to create user/contents.d directory: %w", err)
	}

	contentsFile := filepath.Join(userContentsDPath, serviceName)
	if _, err := os.Create(contentsFile); err != nil {
		return fmt.Errorf("failed to create contents file: %w", err)
	}

	// Create a dependency on base services to prevent race conditions
	dependenciesDPath := filepath.Join(servicePath, "dependencies.d")
	if err := os.MkdirAll(dependenciesDPath, 0755); err != nil {
		return fmt.Errorf("failed to create dependencies.d directory: %w", err)
	}

	baseDepFile := filepath.Join(dependenciesDPath, "base")
	if _, err := os.Create(baseDepFile); err != nil {
		return fmt.Errorf("failed to create base dependency file: %w", err)
	}

	return nil
}

// createRunScript creates a run script for the service
func (s *DefaultService) createRunScript(servicePath string, command []string, env map[string]string) error {
	runScript := filepath.Join(servicePath, "run")
	f, err := os.Create(runScript)
	if err != nil {
		return fmt.Errorf("failed to create run script: %w", err)
	}
	defer f.Close()

	// Create shebang
	if _, err := f.WriteString("#!/command/execlineb -P\n\n"); err != nil {
		return fmt.Errorf("failed to write shebang: %w", err)
	}

	// Add environment variables
	if env != nil && len(env) > 0 {
		for k, v := range env {
			if _, err := f.WriteString(fmt.Sprintf("export %s=\"%s\"\n", k, v)); err != nil {
				return fmt.Errorf("failed to write environment variable: %w", err)
			}
		}
		// Add an empty line
		if _, err := f.WriteString("\n"); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
	}

	// Send stdout/stderr to s6-log if needed
	if _, err := f.WriteString("exec 2>&1\n"); err != nil {
		return fmt.Errorf("failed to write stderr redirection: %w", err)
	}

	// Build command string
	cmdStr := strings.Join(command, " ")
	if _, err := f.WriteString("exec " + cmdStr); err != nil {
		return fmt.Errorf("failed to write command: %w", err)
	}

	// Make run script executable
	if err := os.Chmod(runScript, 0755); err != nil {
		return fmt.Errorf("failed to make run script executable: %w", err)
	}

	return nil
}

// createConfigFiles creates config files needed by the service
func (s *DefaultService) createConfigFiles(servicePath string, configFiles map[string]string) error {
	if configFiles == nil || len(configFiles) == 0 {
		return nil
	}

	for path, content := range configFiles {
		// If path is relative, make it relative to service directory
		if !filepath.IsAbs(path) {
			path = filepath.Join(servicePath, path)
		}

		// Create directory if it doesn't exist
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory for config file: %w", err)
		}

		// Create and write the file
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("failed to create config file %s: %w", path, err)
		}

		if _, err := f.WriteString(content); err != nil {
			f.Close()
			return fmt.Errorf("failed to write to config file %s: %w", path, err)
		}
		f.Close()
	}

	return nil
}

// Remove removes the S6 service directory structure
func (s *DefaultService) Remove(ctx context.Context, servicePath string) error {
	// Remove the service from contents.d first
	serviceName := filepath.Base(servicePath)
	contentsFile := filepath.Join(filepath.Dir(servicePath), "user", "contents.d", serviceName)
	os.Remove(contentsFile) // Ignore errors - file might not exist

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

// Helper function to parse PID
func parsePid(pidStr string) (int, error) {
	var pid int
	_, err := fmt.Sscanf(pidStr, "%d", &pid)
	return pid, err
}
