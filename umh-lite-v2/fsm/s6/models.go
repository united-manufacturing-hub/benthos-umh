package s6

import "context"

// S6ServiceStatus represents the status of a service managed by s6-overlay
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

// S6Service defines the interface for interacting with s6-overlay services
type S6Service interface {
	// Start attempts to start the service
	Start(ctx context.Context) error
	// Stop attempts to stop the service
	Stop(ctx context.Context) error
	// IsRunning returns true if the service is running
	IsRunning() bool
	// GetStatus returns the current status of the service
	GetStatus() S6ServiceStatus
}

// ServiceManager defines the interface for managing Benthos services
type ServiceManager interface {
	// Start starts a Benthos service with the given ID
	Start(ctx context.Context, id string) error
	// Stop stops a Benthos service with the given ID
	Stop(ctx context.Context, id string) error
	// Update updates a Benthos service configuration and restarts it
	Update(ctx context.Context, id string, content string) error
	// WriteConfig writes a configuration to disk and returns the file path
	WriteConfig(id string, content string) (string, error)
	// RegisterService registers an S6Service for a Benthos instance
	RegisterService(id string, service S6Service)
	// GetService retrieves an S6Service by instance ID
	GetService(id string) (S6Service, bool)
	// IsRunning checks if a service is running
	IsRunning(id string) bool
}
