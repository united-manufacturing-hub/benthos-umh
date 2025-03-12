package models

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// S6Service defines the interface for interacting with s6-supervised services
type S6Service interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	IsRunning() bool
	GetStatus() S6ServiceStatus
}

// BenthosServiceManager manages Benthos instances under s6-overlay
type BenthosServiceManager struct {
	// ConfigDir is the directory for Benthos configuration files
	ConfigDir string

	// mu protects the services map
	mu sync.RWMutex

	// Services maps instance IDs to s6 service instances
	Services map[string]S6Service
}

// NewBenthosServiceManager creates a new service manager
func NewBenthosServiceManager(configDir string) (*BenthosServiceManager, error) {
	// Ensure config directory exists
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create config directory: %w", err)
	}

	return &BenthosServiceManager{
		ConfigDir: configDir,
		Services:  make(map[string]S6Service),
	}, nil
}

// WriteConfig writes a Benthos configuration to disk
func (m *BenthosServiceManager) WriteConfig(id string, content string) (string, error) {
	configPath := filepath.Join(m.ConfigDir, fmt.Sprintf("%s.yaml", id))
	if err := ioutil.WriteFile(configPath, []byte(content), 0644); err != nil {
		return "", fmt.Errorf("failed to write config file: %w", err)
	}
	return configPath, nil
}

// RegisterService registers an S6 service for a Benthos instance
func (m *BenthosServiceManager) RegisterService(id string, service S6Service) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Services[id] = service
}

// GetService returns the S6 service for a Benthos instance
func (m *BenthosServiceManager) GetService(id string) (S6Service, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	service, exists := m.Services[id]
	return service, exists
}

// Start starts a Benthos service
func (m *BenthosServiceManager) Start(ctx context.Context, id string) error {
	service, exists := m.GetService(id)
	if !exists {
		return fmt.Errorf("service %s not found", id)
	}

	if service.IsRunning() {
		return fmt.Errorf("service %s is already running", id)
	}

	return service.Start(ctx)
}

// Stop stops a Benthos service
func (m *BenthosServiceManager) Stop(ctx context.Context, id string) error {
	service, exists := m.GetService(id)
	if !exists {
		return fmt.Errorf("service %s not found", id)
	}

	if !service.IsRunning() {
		return fmt.Errorf("service %s is not running", id)
	}

	return service.Stop(ctx)
}

// IsRunning checks if a Benthos service is running
func (m *BenthosServiceManager) IsRunning(id string) bool {
	service, exists := m.GetService(id)
	if !exists {
		return false
	}

	return service.IsRunning()
}

// Update updates a Benthos service with new configuration
func (m *BenthosServiceManager) Update(ctx context.Context, id string, content string) error {
	// Write the new configuration to disk
	_, err := m.WriteConfig(id, content)
	if err != nil {
		return err
	}

	// Stop the service if it's running
	service, exists := m.GetService(id)
	if exists && service.IsRunning() {
		if err := service.Stop(ctx); err != nil {
			return fmt.Errorf("failed to stop service %s: %w", id, err)
		}

		// Wait for the service to stop
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if !service.IsRunning() {
					// Service has stopped, we can start it again
					break
				}
			case <-waitCtx.Done():
				return fmt.Errorf("timeout waiting for service %s to stop", id)
			}

			// If we've exited the loop, break out of the select
			if !service.IsRunning() {
				break
			}
		}
	}

	// Start the service with the new configuration
	return m.Start(ctx, id)
}

// GetAllRunningServices returns a list of all running service IDs
func (m *BenthosServiceManager) GetAllRunningServices() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var runningServices []string
	for id, service := range m.Services {
		if service.IsRunning() {
			runningServices = append(runningServices, id)
		}
	}

	return runningServices
}
