package benthosfsm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/looplab/fsm"
)

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

// NewBenthosConfig creates a new configuration with version based on content hash
func NewBenthosConfig(content string) *BenthosConfig {
	now := time.Now()
	hash := sha256.Sum256([]byte(content))

	return &BenthosConfig{
		Content:    content,
		Version:    hex.EncodeToString(hash[:]),
		CreateTime: now,
		UpdateTime: now,
	}
}

// SetConfigPath updates the configuration path
func (c *BenthosConfig) SetConfigPath(path string) {
	c.ConfigPath = path
	c.UpdateTime = time.Now()
}

// Equal checks if two BenthosConfigs are functionally equivalent
func (c *BenthosConfig) Equal(other *BenthosConfig) bool {
	if c == nil || other == nil {
		return c == other
	}

	// Compare content - this is the most important part
	return c.Content == other.Content
}

// StartBenthos is an action to start a Benthos instance
func StartBenthos(ctx context.Context, instance *BenthosInstance, manager ServiceManager) error {
	if instance.GetState() != StateStarting {
		return fmt.Errorf("cannot start instance in state %s", instance.GetState())
	}

	// Get the service for this instance
	service, exists := manager.GetService(instance.ID)
	if !exists {
		return errors.New("no service registered for instance")
	}

	// If the service is already running, we don't need to start it
	if service.IsRunning() {
		return instance.SendEvent(ctx, EventStarted)
	}

	// Start the service
	err := manager.Start(ctx, instance.ID)
	if err != nil {
		// Check if this might be a configuration error
		if isConfigError(err) {
			// If it looks like a configuration error, set it as such
			instance.SetConfigError(err)
			return instance.SendEvent(ctx, EventConfigError, err)
		} else {
			// Otherwise, treat it as a generic error
			instance.SetError(err)
			return instance.SendEvent(ctx, EventFail, err)
		}
	}

	// Send the started event
	return instance.SendEvent(ctx, EventStarted)
}

// isConfigError tries to determine if an error is related to configuration
func isConfigError(err error) bool {
	if err == nil {
		return false
	}

	// Look for common configuration error indicators in the error message
	errMsg := err.Error()
	configErrorPatterns := []string{
		"config", "configuration", "yaml", "json", "syntax",
		"parse", "invalid", "schema", "validation", "format",
	}

	for _, pattern := range configErrorPatterns {
		if strings.Contains(strings.ToLower(errMsg), pattern) {
			return true
		}
	}

	return false
}

// StopBenthos is an action to stop a Benthos instance
func StopBenthos(ctx context.Context, instance *BenthosInstance, manager ServiceManager) error {
	// Get the service for this instance
	service, exists := manager.GetService(instance.ID)
	if !exists {
		return errors.New("no service registered for instance")
	}

	// If the service is already stopped, we don't need to stop it
	if !service.IsRunning() {
		return nil
	}

	// Stop the service
	return manager.Stop(ctx, instance.ID)
}

// UpdateBenthosConfig is an action to update a Benthos instance configuration
func UpdateBenthosConfig(ctx context.Context, instance *BenthosInstance, manager ServiceManager) error {
	if instance.GetState() != StateChangingConfig {
		return fmt.Errorf("cannot update config in state %s", instance.GetState())
	}

	if instance.DesiredConfig == nil {
		return errors.New("no desired configuration set")
	}

	// Update the service configuration
	err := manager.Update(ctx, instance.ID, instance.DesiredConfig.Content)
	if err != nil {
		instance.SetConfigError(err)
		return instance.SendEvent(ctx, EventConfigError, err)
	}

	// Add the current config to history before replacing it
	if instance.CurrentConfig != nil {
		instance.AddToConfigHistory(instance.CurrentConfig, ConfigStatusSuccess)
	}
	// Update the current configuration
	instance.mu.Lock()
	instance.CurrentConfig = instance.DesiredConfig
	instance.DesiredConfig = nil
	instance.mu.Unlock()

	// Send the update done event
	return instance.SendEvent(ctx, EventUpdateDone)
}

// RollbackBenthosConfig is an action to roll back to a previous successful configuration
func RollbackBenthosConfig(ctx context.Context, instance *BenthosInstance, manager ServiceManager) error {
	if instance.GetState() != StateChangingConfig {
		return fmt.Errorf("cannot rollback config in state %s", instance.GetState())
	}

	// Get the latest successful configuration
	previousConfig := instance.GetLatestSuccessfulConfig()
	if previousConfig == nil {
		// No previous successful configuration to roll back to
		instance.SetError(errors.New("no previous successful configuration found for rollback"))
		return instance.SendEvent(ctx, EventFail, "no previous config")
	}

	// Attempt to update the service with the previous configuration
	err := manager.Update(ctx, instance.ID, previousConfig.Content)
	if err != nil {
		instance.SetError(fmt.Errorf("rollback failed: %w", err))
		return instance.SendEvent(ctx, EventFail, err)
	}

	// Update the current configuration
	instance.mu.Lock()
	instance.CurrentConfig = previousConfig
	instance.DesiredConfig = nil
	instance.ClearConfigError() // Clear configuration error since rollback succeeded
	instance.mu.Unlock()

	// Send the rollback done event
	return instance.SendEvent(ctx, EventRollbackDone)
}

// HandleConfigErrorState is an action to handle configuration errors
func HandleConfigErrorState(ctx context.Context, instance *BenthosInstance) error {
	if instance.GetState() != StateConfigError {
		return fmt.Errorf("cannot handle config error in state %s", instance.GetState())
	}

	// Log the configuration error
	configError := instance.GetConfigError()
	if configError != nil {
		log.Printf("Configuration error for instance %s: %v", instance.ID, configError)
	}

	// Initiate rollback automatically
	return instance.SendEvent(ctx, EventRollback)
}

// HandleErrorState is an action to handle an instance in the error state
func HandleErrorState(ctx context.Context, instance *BenthosInstance) error {
	if instance.GetState() != StateError {
		return fmt.Errorf("cannot handle error in state %s", instance.GetState())
	}

	// Check if it's time to retry
	if instance.ShouldRetry() {
		instance.ClearError()
		return instance.SendEvent(ctx, EventRetry)
	}

	return nil
}

// ReconcileInstance ensures the instance's actual state aligns with its desired state
func ReconcileInstance(ctx context.Context, instance *BenthosInstance, manager ServiceManager) error {
	currentState := instance.GetState()
	desiredState := instance.GetDesiredState()

	// If the instance is in an error state, handle it first
	if currentState == StateError {
		return HandleErrorState(ctx, instance)
	}

	// If the instance is in a config error state, handle it first
	if currentState == StateConfigError {
		return HandleConfigErrorState(ctx, instance)
	}

	// Handle configuration changes first if needed
	if currentState == StateRunning && instance.NeedsConfigChange() {
		return instance.SendEvent(ctx, EventConfigChange)
	} else if currentState == StateConfigChanged {
		return instance.SendEvent(ctx, EventUpdateConfig)
	} else if currentState == StateChangingConfig {
		return UpdateBenthosConfig(ctx, instance, manager)
	}

	// If the instance is already in the desired state, nothing to do
	if currentState == desiredState {
		return nil
	}

	// Transitions to get to the desired state
	switch desiredState {
	case StateRunning:
		if currentState == StateStopped {
			return instance.SendEvent(ctx, EventStart)
		} else if currentState == StateStarting {
			return StartBenthos(ctx, instance, manager)
		}
	case StateStopped:
		if currentState == StateRunning {
			return instance.SendEvent(ctx, EventStop)
		}
	}

	return nil
}

// RegisterCallbacks registers common callbacks for state transitions
func RegisterCallbacks(instance *BenthosInstance, manager ServiceManager) {
	// Register callbacks for state transitions
	instance.callbacks["enter_"+StateStarting] = func(ctx context.Context, e *fsm.Event) {
		// Execute synchronously to avoid lock contention
		err := StartBenthos(ctx, instance, manager)
		if err != nil {
			instance.SetError(err)
		}
	}

	instance.callbacks["enter_"+StateStopped] = func(ctx context.Context, e *fsm.Event) {
		// Execute synchronously to avoid lock contention
		err := StopBenthos(ctx, instance, manager)
		if err != nil {
			instance.SetError(err)
		}
	}

	instance.callbacks["enter_"+StateChangingConfig] = func(ctx context.Context, e *fsm.Event) {
		// Execute synchronously to avoid lock contention
		if instance.GetState() == StateChangingConfig && instance.FSM.Current() == StateChangingConfig {
			// Check the previous state to determine which action to take
			previousEvent := "unknown"
			if len(e.Args) > 0 {
				if eventName, ok := e.Args[0].(string); ok {
					previousEvent = eventName
				}
			}

			var err error
			if previousEvent == EventRollback || instance.GetConfigError() != nil {
				// This is a rollback operation
				err = RollbackBenthosConfig(ctx, instance, manager)
			} else {
				// This is a normal configuration update
				err = UpdateBenthosConfig(ctx, instance, manager)
			}

			if err != nil {
				instance.SetError(err)
			}
		}
	}

	instance.callbacks["enter_"+StateConfigError] = func(ctx context.Context, e *fsm.Event) {
		// Log the configuration error if any is provided
		if len(e.Args) > 0 {
			if err, ok := e.Args[0].(error); ok {
				instance.SetConfigError(err)
				log.Printf("Instance %s entered config error state: %v", instance.ID, err)

				// Add the failed config to history
				if instance.DesiredConfig != nil {
					instance.AddToConfigHistory(instance.DesiredConfig, ConfigStatusError)
				}
			}
		}

		// Schedule automatic rollback with a brief delay
		time.AfterFunc(1*time.Second, func() {
			err := HandleConfigErrorState(ctx, instance)
			if err != nil {
				log.Printf("Error handling config error state: %v", err)
			}
		})
	}

	instance.callbacks["enter_"+StateError] = func(ctx context.Context, e *fsm.Event) {
		// Log the error if any is provided
		if len(e.Args) > 0 {
			if err, ok := e.Args[0].(error); ok {
				instance.SetError(err)
				log.Printf("Instance %s entered error state: %v", instance.ID, err)
			}
		}
	}

	instance.callbacks["enter_"+StateRunning] = func(ctx context.Context, e *fsm.Event) {
		// If we successfully transitioned to running and have a current config,
		// mark it as successful in the history
		if instance.CurrentConfig != nil {
			instance.AddToConfigHistory(instance.CurrentConfig, ConfigStatusSuccess)
			log.Printf("Instance %s successfully running with config version %s",
				instance.ID, instance.CurrentConfig.Version)
		}
	}
}
