package s6

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"text/template"

	filesystem "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/pkg/service/filesystem"
)

// S6ServiceConfig contains configuration for creating a service
type S6ServiceConfig struct {
	Command     []string          `yaml:"command"`
	Env         map[string]string `yaml:"env"`
	ConfigFiles map[string]string `yaml:"configFiles"`
}

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
	Status    ServiceStatus
	Uptime    int64 // seconds the service has been up
	DownTime  int64 // seconds the service has been down
	ReadyTime int64 // seconds the service has been ready
	Pid       int   // process ID if service is up
	ExitCode  int   // exit code if service is down
	WantUp    bool  // whether the service wants to be up
	// History of exit codes
	ExitHistory []ExitEvent
}

// ExitEvent represents a service exit event
type ExitEvent struct {
	Timestamp string // timestamp of the exit event
	ExitCode  int    // exit code of the service
}

// Service defines the interface for interacting with S6 services
type Service interface {
	// Create creates the service with specific configuration
	Create(ctx context.Context, servicePath string, config S6ServiceConfig) error
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
	ServiceExists(ctx context.Context, servicePath string) (bool, error)
	// GetConfig gets the actual service config from s6
	GetConfig(ctx context.Context, servicePath string) (S6ServiceConfig, error)
}

// DefaultService is the default implementation of the S6 Service interface
type DefaultService struct {
	fsService filesystem.Service
}

// NewDefaultService creates a new default S6 service
func NewDefaultService() Service {
	return &DefaultService{
		fsService: filesystem.NewDefaultService(),
	}
}

// WithFileSystemService sets a custom filesystem service for the S6 service
func (s *DefaultService) WithFileSystemService(fsService filesystem.Service) *DefaultService {
	s.fsService = fsService
	return s
}

// Create creates the S6 service with specific configuration
func (s *DefaultService) Create(ctx context.Context, servicePath string, config S6ServiceConfig) error {
	log.Printf("[S6Service] Creating S6 service %s", servicePath)

	// Create service directory if it doesn't exist
	if err := s.fsService.EnsureDirectory(ctx, servicePath); err != nil {
		return fmt.Errorf("failed to create service directory: %w", err)
	}

	// Create down file to prevent automatic startup
	downFilePath := filepath.Join(servicePath, "down")
	exists, err := s.fsService.FileExists(ctx, downFilePath)
	if err != nil {
		return fmt.Errorf("failed to check if down file exists: %w", err)
	}
	if !exists {
		f, err := s.fsService.CreateFile(ctx, downFilePath, 0644)
		if err != nil {
			return fmt.Errorf("failed to create down file: %w", err)
		}

		closeErr := f.Close()
		if closeErr != nil {
			return fmt.Errorf("failed to close down file: %w", closeErr)
		}
	}

	// Create type file (required for s6-rc)
	typeFile := filepath.Join(servicePath, "type")
	exists, err = s.fsService.FileExists(ctx, typeFile)
	if err != nil {
		return fmt.Errorf("failed to check if type file exists: %w", err)
	}
	if !exists {
		f, err := s.fsService.CreateFile(ctx, typeFile, 0644)
		if err != nil {
			return fmt.Errorf("failed to create type file: %w", err)
		}

		writeErr := errors.New("")
		if _, writeErr = f.WriteString("longrun"); writeErr != nil {
			closeErr := f.Close()
			return errors.Join(
				fmt.Errorf("failed to write to type file: %w", writeErr),
				errors.New(fmt.Sprintf("additional error when closing file: %v", closeErr)),
			)
		}

		closeErr := f.Close()
		if closeErr != nil {
			return fmt.Errorf("failed to close type file: %w", closeErr)
		}
	}

	// s6-supervise requires a run script to function properly
	if len(config.Command) > 0 {
		if err := s.createS6RunScript(ctx, servicePath, config.Command, config.Env); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("no command specified for service %s", servicePath)
	}

	// Create any config files specified
	if err := s.createS6ConfigFiles(ctx, servicePath, config.ConfigFiles); err != nil {
		return err
	}

	// Register service in user/contents.d
	serviceName := filepath.Base(servicePath)
	userContentsDPath := filepath.Join(filepath.Dir(servicePath), "user", "contents.d")
	if err := s.fsService.EnsureDirectory(ctx, userContentsDPath); err != nil {
		return fmt.Errorf("failed to create user/contents.d directory: %w", err)
	}

	contentsFile := filepath.Join(userContentsDPath, serviceName)
	f, err := s.fsService.CreateFile(ctx, contentsFile, 0644)
	if err != nil {
		return fmt.Errorf("failed to create contents file: %w", err)
	}

	closeErr := f.Close()
	if closeErr != nil {
		return fmt.Errorf("failed to close contents file: %w", closeErr)
	}

	// Create a dependency on base services to prevent race conditions
	dependenciesDPath := filepath.Join(servicePath, "dependencies.d")
	if err := s.fsService.EnsureDirectory(ctx, dependenciesDPath); err != nil {
		return fmt.Errorf("failed to create dependencies.d directory: %w", err)
	}

	baseDepFile := filepath.Join(dependenciesDPath, "base")
	f, err = s.fsService.CreateFile(ctx, baseDepFile, 0644)
	if err != nil {
		return fmt.Errorf("failed to create base dependency file: %w", err)
	}

	closeErr = f.Close()
	if closeErr != nil {
		return fmt.Errorf("failed to close base dependency file: %w", closeErr)
	}

	// if the supervise directory does not exist, notify s6-svscan
	superviseDir := filepath.Join(servicePath, "supervise")
	exists, err = s.fsService.FileExists(ctx, superviseDir)
	if err != nil {
		return fmt.Errorf("failed to check if supervise directory exists: %w", err)
	}

	if !exists {
		output, err := s.fsService.ExecuteCommand(ctx, "s6-svscanctl", "-a", "/run/service")
		if err != nil {
			return fmt.Errorf("failed to notify s6-svscan: %w, output: %s", err, string(output))
		}
	}

	log.Printf("[S6Service] S6 service %s created", servicePath)

	return nil
}

// createRunScript creates a run script for the service
func (s *DefaultService) createS6RunScript(ctx context.Context, servicePath string, command []string, env map[string]string) error {
	runScript := filepath.Join(servicePath, "run")
	f, err := s.fsService.CreateFile(ctx, runScript, 0644)
	if err != nil {
		return fmt.Errorf("failed to create run script: %w", err)
	}

	// Use deferred close with error handling
	defer func() {
		closeErr := f.Close()
		if closeErr != nil && err == nil {
			err = fmt.Errorf("failed to close run script: %w", closeErr)
		} else if closeErr != nil {
			err = errors.Join(err, fmt.Errorf("additional error when closing run script: %w", closeErr))
		}
	}()

	// Create template data
	data := struct {
		Command []string
		Env     map[string]string
	}{
		Command: command,
		Env:     env,
	}

	// Parse and execute the template
	tmpl, err := template.New("runscript").Parse(runScriptTemplate)
	if err != nil {
		return fmt.Errorf("failed to parse run script template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return fmt.Errorf("failed to execute run script template: %w", err)
	}

	// Write the templated content to the file
	if _, err := f.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to write run script: %w", err)
	}

	// Make run script executable
	if err := s.fsService.Chmod(ctx, runScript, 0755); err != nil {
		return fmt.Errorf("failed to make run script executable: %w", err)
	}

	return err // Return the error that may have been set in the deferred close
}

// createConfigFiles creates config files needed by the service
func (s *DefaultService) createS6ConfigFiles(ctx context.Context, servicePath string, configFiles map[string]string) error {
	if len(configFiles) == 0 {
		return nil
	}

	configPath := filepath.Join(servicePath, "config")

	for path, content := range configFiles {
		// If path is relative, make it relative to service directory
		if !filepath.IsAbs(path) {
			path = filepath.Join(configPath, path)
		}

		// Create directory if it doesn't exist
		dir := filepath.Dir(path)
		if err := s.fsService.EnsureDirectory(ctx, dir); err != nil {
			return fmt.Errorf("failed to create directory for config file: %w", err)
		}

		// Create and write the file
		if err := s.fsService.WriteFile(ctx, path, []byte(content), 0644); err != nil {
			return fmt.Errorf("failed to write to config file %s: %w", path, err)
		}
	}

	return nil
}

// Remove removes the S6 service directory structure
func (s *DefaultService) Remove(ctx context.Context, servicePath string) error {
	log.Printf("[S6Service] Removing S6 service %s", servicePath)
	exists, err := s.ServiceExists(ctx, servicePath)
	if err != nil {
		return err
	}
	if !exists {
		return ErrServiceNotExist
	}

	// Remove the service from contents.d first
	serviceName := filepath.Base(servicePath)
	contentsFile := filepath.Join(filepath.Dir(servicePath), "user", "contents.d", serviceName)
	s.fsService.Remove(ctx, contentsFile) // Ignore errors - file might not exist

	err = s.fsService.RemoveAll(ctx, servicePath)
	if err != nil {
		return fmt.Errorf("failed to remove S6 service %s: %w", servicePath, err)
	}

	log.Printf("[S6Service] Removed S6 service %s from contents.d", servicePath)
	return nil
}

// Start starts the S6 service
func (s *DefaultService) Start(ctx context.Context, servicePath string) error {
	log.Printf("[S6Service] Starting S6 service %s", servicePath)
	exists, err := s.ServiceExists(ctx, servicePath)
	if err != nil {
		return err
	}
	if !exists {
		return ErrServiceNotExist
	}

	output, err := s.fsService.ExecuteCommand(ctx, "s6-svc", "-u", servicePath)
	if err != nil {
		return fmt.Errorf("failed to start service: %w, output: %s", err, string(output))
	}
	log.Printf("[S6Service] Started S6 service %s", servicePath)
	return nil
}

// Stop stops the S6 service
func (s *DefaultService) Stop(ctx context.Context, servicePath string) error {
	log.Printf("[S6Service] Stopping S6 service %s", servicePath)
	exists, err := s.ServiceExists(ctx, servicePath)
	if err != nil {
		return err
	}
	if !exists {
		return ErrServiceNotExist
	}

	output, err := s.fsService.ExecuteCommand(ctx, "s6-svc", "-d", servicePath)
	if err != nil {
		return fmt.Errorf("failed to stop service: %w, output: %s", err, string(output))
	}
	log.Printf("[S6Service] Stopped S6 service %s", servicePath)
	return nil
}

// Restart restarts the S6 service
func (s *DefaultService) Restart(ctx context.Context, servicePath string) error {
	log.Printf("[S6Service] Restarting S6 service %s", servicePath)
	exists, err := s.ServiceExists(ctx, servicePath)
	if err != nil {
		return err
	}
	if !exists {
		return ErrServiceNotExist
	}

	output, err := s.fsService.ExecuteCommand(ctx, "s6-svc", "-r", servicePath)
	if err != nil {
		return fmt.Errorf("failed to restart service: %w, output: %s", err, string(output))
	}
	log.Printf("[S6Service] Restarted S6 service %s", servicePath)
	return nil
}

// Status gets the current status of the S6 service
func (s *DefaultService) Status(ctx context.Context, servicePath string) (ServiceInfo, error) {
	info := ServiceInfo{
		Status: ServiceUnknown,
	}

	// Check if service directory exists
	exists, err := s.ServiceExists(ctx, servicePath)
	if err != nil {
		return info, err
	}
	if !exists {
		return info, ErrServiceNotExist
	}

	output, err := s.fsService.ExecuteCommand(ctx, "s6-svstat", servicePath)
	if err != nil {
		return info, fmt.Errorf("failed to get status: %w, output: %s", err, string(output))
	}

	outputStr := string(output)

	// Parse the output from s6-svstat
	// Example outputs:
	// "up (pid 123) 45 seconds, ready 40 seconds"
	// "down (exitcode 100) 6 seconds, ready 6 seconds"
	// "down (exitcode 100) 0 seconds, want up, ready 0 seconds"

	if strings.Contains(outputStr, "up") {
		info.Status = ServiceUp

		// Extract PID
		if pidIndex := strings.Index(outputStr, "pid "); pidIndex >= 0 {
			endIndex := strings.Index(outputStr[pidIndex+4:], ")") + pidIndex + 4
			if endIndex > pidIndex+4 {
				pidStr := outputStr[pidIndex+4 : endIndex]
				info.Pid, _ = strconv.Atoi(pidStr)
			}
		}

		// Extract uptime
		if uptimeIndex := strings.Index(outputStr, ") "); uptimeIndex >= 0 {
			endIndex := strings.Index(outputStr[uptimeIndex+2:], " seconds")
			if endIndex > 0 {
				uptimeStr := outputStr[uptimeIndex+2 : uptimeIndex+2+endIndex]
				info.Uptime, _ = strconv.ParseInt(uptimeStr, 10, 64)
			}
		}
	} else if strings.Contains(outputStr, "down") {
		info.Status = ServiceDown

		// Extract exit code
		if exitIndex := strings.Index(outputStr, "exitcode "); exitIndex >= 0 {
			endIndex := strings.Index(outputStr[exitIndex+9:], ")") + exitIndex + 9
			if endIndex > exitIndex+9 {
				exitStr := outputStr[exitIndex+9 : endIndex]
				info.ExitCode, _ = strconv.Atoi(exitStr)
			}
		}

		// Extract downtime
		if downtimeIndex := strings.Index(outputStr, ") "); downtimeIndex >= 0 {
			endIndex := strings.Index(outputStr[downtimeIndex+2:], " seconds")
			if endIndex > 0 {
				downtimeStr := outputStr[downtimeIndex+2 : downtimeIndex+2+endIndex]
				info.DownTime, _ = strconv.ParseInt(downtimeStr, 10, 64)
			}
		}
	} else if strings.Contains(outputStr, "restarting") {
		info.Status = ServiceRestarting
	} else {
		info.Status = ServiceFailed
	}

	// Check if service wants to be up
	info.WantUp = strings.Contains(outputStr, "want up")

	// Extract ready time
	if readyIndex := strings.Index(outputStr, "ready "); readyIndex >= 0 {
		endIndex := strings.Index(outputStr[readyIndex+6:], " seconds")
		if endIndex > 0 {
			readyStr := outputStr[readyIndex+6 : readyIndex+6+endIndex]
			info.ReadyTime, _ = strconv.ParseInt(readyStr, 10, 64)
		}
	}

	// Use full path to s6-svdt and point to the supervision directory
	detailOutput, err := s.fsService.ExecuteCommand(ctx, "s6-svdt", servicePath)
	if err == nil {
		// Parse output from s6-svdt
		// Example output:
		// @4000000067d1fcf003db4342 exitcode 100
		// @4000000067d1fcf104d3c3a2 exitcode 100

		info.ExitHistory = parseExitHistory(string(detailOutput))
	}

	//log.Printf("[S6Service] Status fetched for S6 service %s: %+v", servicePath, info)

	return info, nil
}

// parseExitHistory parses the output of s6-svdt to extract exit history
func parseExitHistory(output string) []ExitEvent {
	var history []ExitEvent

	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}

		// Parse timestamp and exit code
		parts := strings.Fields(line)
		if len(parts) >= 3 && parts[1] == "exitcode" {
			event := ExitEvent{
				Timestamp: parts[0],
				ExitCode:  0,
			}

			code, err := strconv.Atoi(parts[2])
			if err == nil {
				event.ExitCode = code
			}

			history = append(history, event)
		}
	}

	return history
}

// ServiceExists checks if the service directory exists
func (s *DefaultService) ServiceExists(ctx context.Context, servicePath string) (bool, error) {
	exists, err := s.fsService.FileExists(ctx, servicePath)
	if err != nil {
		log.Printf("[S6Service] Error checking if S6 service %s exists: %v", servicePath, err)
		return false, err
	}
	if !exists {
		log.Printf("[S6Service] S6 service %s does not exist", servicePath)
		return false, nil
	}
	return true, nil
}

// GetConfig gets the actual service config from s6
func (s *DefaultService) GetConfig(ctx context.Context, servicePath string) (S6ServiceConfig, error) {
	exists, err := s.ServiceExists(ctx, servicePath)
	if err != nil {
		return S6ServiceConfig{}, err
	}
	if !exists {
		return S6ServiceConfig{}, ErrServiceNotExist
	}

	observedS6ServiceConfig := S6ServiceConfig{
		ConfigFiles: make(map[string]string),
		Env:         make(map[string]string),
	}

	// Fetch run script
	runScript := filepath.Join(servicePath, "run")
	content, err := s.fsService.ReadFile(ctx, runScript)
	if err != nil {
		return S6ServiceConfig{}, fmt.Errorf("failed to read run script: %w", err)
	}

	// Parse the run script content
	scriptContent := string(content)

	// Extract environment variables using regex
	envMatches := envVarParser.FindAllStringSubmatch(scriptContent, -1)
	for _, match := range envMatches {
		if len(match) == 3 {
			key := match[1]
			value := strings.TrimSpace(match[2])
			// Remove any quotes
			value = strings.Trim(value, "\"'")
			observedS6ServiceConfig.Env[key] = value
		}
	}

	// Extract command using regex
	cmdMatch := runScriptParser.FindStringSubmatch(scriptContent)

	if len(cmdMatch) >= 2 && cmdMatch[1] != "" {
		// If we captured the command on the same line as fdmove
		cmdLine := strings.TrimSpace(cmdMatch[1])
		observedS6ServiceConfig.Command = parseCommandLine(cmdLine)
	} else {
		// If the command is on the line after fdmove, or regex didn't match properly
		lines := strings.Split(scriptContent, "\n")
		var commandLine string

		// Find the fdmove line
		for i, line := range lines {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "fdmove") {
				// Check if command is on the same line after fdmove
				if strings.Contains(line, " ") && len(line) > strings.LastIndex(line, " ")+1 {
					parts := strings.SplitN(line, "fdmove -c 2 1", 2)
					if len(parts) > 1 && strings.TrimSpace(parts[1]) != "" {
						commandLine = strings.TrimSpace(parts[1])
						break
					}
				}

				// Otherwise, look for first non-empty line after fdmove
				for j := i + 1; j < len(lines); j++ {
					nextLine := strings.TrimSpace(lines[j])
					if nextLine != "" {
						commandLine = nextLine
						break
					}
				}

				if commandLine != "" {
					break
				}
			}
		}

		if commandLine != "" {
			observedS6ServiceConfig.Command = parseCommandLine(commandLine)
		} else {
			// Absolute fallback - try to look for the command we know should be there
			log.Printf("[S6Service] Warning: Could not find command in run script for %s, searching for known paths", servicePath)
			cmdRegex := regexp.MustCompile(`(/[^\s]+)`)
			cmdMatches := cmdRegex.FindAllString(scriptContent, -1)

			if len(cmdMatches) > 0 {
				// Use the first matching path-like string we find as the command
				cmd := cmdMatches[0]
				args := []string{}

				// Look for arguments after the command
				argIndex := strings.Index(scriptContent, cmd) + len(cmd)
				if argIndex < len(scriptContent) {
					argPart := strings.TrimSpace(scriptContent[argIndex:])
					if argPart != "" {
						args = parseCommandLine(argPart)
					}
				}

				observedS6ServiceConfig.Command = append([]string{cmd}, args...)
			}
		}
	}

	// Fetch config files from servicePath
	configPath := filepath.Join(servicePath, "config")
	exists, err = s.fsService.FileExists(ctx, configPath)
	if err != nil {
		return S6ServiceConfig{}, err
	}
	if !exists {
		return observedS6ServiceConfig, nil
	}

	entries, err := s.fsService.ReadDir(ctx, configPath)
	if err != nil {
		return S6ServiceConfig{}, fmt.Errorf("failed to read config directory: %w", err)
	}

	// Extract config files
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filePath := filepath.Join(configPath, entry.Name())
		content, err := s.fsService.ReadFile(ctx, filePath)
		if err != nil {
			return S6ServiceConfig{}, fmt.Errorf("failed to read config file %s: %w", entry.Name(), err)
		}

		observedS6ServiceConfig.ConfigFiles[entry.Name()] = string(content)
	}

	return observedS6ServiceConfig, nil
}

// parseCommandLine splits a command line into command and arguments, respecting quotes
func parseCommandLine(cmdLine string) []string {
	var cmdParts []string
	var currentPart strings.Builder
	inQuote := false

	for i := 0; i < len(cmdLine); i++ {
		if cmdLine[i] == '"' || cmdLine[i] == '\'' {
			inQuote = !inQuote
			continue
		}

		if cmdLine[i] == ' ' && !inQuote {
			if currentPart.Len() > 0 {
				cmdParts = append(cmdParts, currentPart.String())
				currentPart.Reset()
			}
		} else {
			currentPart.WriteByte(cmdLine[i])
		}
	}

	if currentPart.Len() > 0 {
		cmdParts = append(cmdParts, currentPart.String())
	}

	return cmdParts
}

// Equal checks if two S6ServiceConfigs are equal
func (c S6ServiceConfig) Equal(other S6ServiceConfig) bool {
	// Compare Command slices
	if len(c.Command) != len(other.Command) {
		return false
	}
	for i, cmd := range c.Command {
		if cmd != other.Command[i] {
			return false
		}
	}

	// Compare Env maps
	if len(c.Env) != len(other.Env) {
		return false
	}
	for k, v := range c.Env {
		if otherV, ok := other.Env[k]; !ok || v != otherV {
			return false
		}
	}

	// Compare ConfigFiles maps
	if len(c.ConfigFiles) != len(other.ConfigFiles) {
		return false
	}
	for k, v := range c.ConfigFiles {
		if otherV, ok := other.ConfigFiles[k]; !ok || v != otherV {
			return false
		}
	}

	return true
}
