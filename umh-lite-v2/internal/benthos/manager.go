package benthos

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

var (
	// ErrProcessNotFound is returned when a process is not found
	ErrProcessNotFound = errors.New("benthos process not found")
	// ErrStartupTimeout is returned when a process fails to start within the timeout
	ErrStartupTimeout = errors.New("benthos process startup timeout")
	// ErrProcessAlreadyRunning is returned when trying to start an already running process
	ErrProcessAlreadyRunning = errors.New("benthos process already running")
)

// BenthosManager defines the interface for managing Benthos processes
type BenthosManager interface {
	// WriteConfig writes a Benthos configuration to disk
	WriteConfig(id, content string) (string, error)
	// Start starts a Benthos process with the given configuration
	Start(ctx context.Context, id, configPath string) error
	// Stop stops a Benthos process
	Stop(ctx context.Context, id string) error
	// Update updates a Benthos process with new configuration
	Update(ctx context.Context, id, content string) error
	// IsRunning checks if a Benthos process is running
	IsRunning(id string) bool
	// GetAllRunningProcesses returns a list of all running process IDs
	GetAllRunningProcesses() []string
}

// Manager manages the lifecycle of Benthos processes
type Manager struct {
	configDir string
	processes map[string]*Process
	mu        sync.RWMutex
}

// Process represents a running Benthos process
type Process struct {
	ID         string
	ConfigPath string
	Cmd        *exec.Cmd
	StartTime  time.Time
}

// NewManager creates a new Manager for Benthos processes
func NewManager(configDir string) (*Manager, error) {
	// Ensure the config directory exists
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create config directory: %w", err)
	}

	return &Manager{
		configDir: configDir,
		processes: make(map[string]*Process),
	}, nil
}

// WriteConfig writes a Benthos configuration to disk
func (m *Manager) WriteConfig(id, content string) (string, error) {
	configPath := filepath.Join(m.configDir, fmt.Sprintf("%s.yaml", id))
	if err := ioutil.WriteFile(configPath, []byte(content), 0644); err != nil {
		return "", fmt.Errorf("failed to write config file: %w", err)
	}
	return configPath, nil
}

// Start starts a Benthos process with the given configuration
func (m *Manager) Start(ctx context.Context, id, configPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if process already exists
	if proc, exists := m.processes[id]; exists && proc.Cmd != nil && proc.Cmd.Process != nil {
		return ErrProcessAlreadyRunning
	}

	// Create the Benthos command
	cmd := exec.CommandContext(ctx, "benthos", "-c", configPath)

	// Set stdout and stderr
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Start the process
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start benthos process: %w", err)
	}

	// Store the process
	m.processes[id] = &Process{
		ID:         id,
		ConfigPath: configPath,
		Cmd:        cmd,
		StartTime:  time.Now(),
	}

	// Start a goroutine to wait for the process
	go func() {
		err := cmd.Wait()
		m.mu.Lock()
		defer m.mu.Unlock()

		// Remove the process from our map if it has exited
		if proc, exists := m.processes[id]; exists && proc.Cmd == cmd {
			delete(m.processes, id)
		}

		if err != nil && ctx.Err() == nil {
			// Process exited with an error not due to context cancellation
			fmt.Printf("Benthos process %s exited with error: %v\n", id, err)
		}
	}()

	// Check if the process is still running after a short delay
	time.Sleep(200 * time.Millisecond)
	if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
		delete(m.processes, id)
		return fmt.Errorf("benthos process exited immediately: %v", cmd.ProcessState)
	}

	return nil
}

// Stop stops a Benthos process
func (m *Manager) Stop(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	proc, exists := m.processes[id]
	if !exists || proc.Cmd == nil || proc.Cmd.Process == nil {
		return ErrProcessNotFound
	}

	// First try graceful shutdown with SIGTERM
	if err := proc.Cmd.Process.Signal(syscall.SIGTERM); err != nil {
		// If SIGTERM fails, try SIGKILL
		if err := proc.Cmd.Process.Kill(); err != nil {
			return fmt.Errorf("failed to kill benthos process: %w", err)
		}
	}

	// Wait for the process to exit with a timeout
	timer := time.NewTimer(5 * time.Second)
	stopped := make(chan struct{})

	go func() {
		// This goroutine will stop when the process exits via the Wait in Start method
		for {
			m.mu.RLock()
			_, exists := m.processes[id]
			m.mu.RUnlock()

			if !exists {
				close(stopped)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	select {
	case <-stopped:
		timer.Stop()
		return nil
	case <-timer.C:
		// Timeout, force kill
		m.mu.Lock()
		if proc, exists := m.processes[id]; exists && proc.Cmd != nil && proc.Cmd.Process != nil {
			if err := proc.Cmd.Process.Kill(); err != nil {
				m.mu.Unlock()
				return fmt.Errorf("failed to force kill benthos process: %w", err)
			}
			delete(m.processes, id)
		}
		m.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Update updates a Benthos process with new configuration
func (m *Manager) Update(ctx context.Context, id, content string) error {
	// Write the new configuration to disk
	configPath, err := m.WriteConfig(id, content)
	if err != nil {
		return err
	}

	// Stop the existing process if running
	_ = m.Stop(ctx, id) // Ignore error if process doesn't exist

	// Start a new process with the updated configuration
	return m.Start(ctx, id, configPath)
}

// IsRunning checks if a Benthos process is running
func (m *Manager) IsRunning(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	proc, exists := m.processes[id]
	return exists && proc.Cmd != nil && proc.Cmd.Process != nil && proc.Cmd.ProcessState == nil
}

// GetAllRunningProcesses returns a list of all running process IDs
func (m *Manager) GetAllRunningProcesses() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var ids []string
	for id, proc := range m.processes {
		if proc.Cmd != nil && proc.Cmd.Process != nil && proc.Cmd.ProcessState == nil {
			ids = append(ids, id)
		}
	}
	return ids
}
