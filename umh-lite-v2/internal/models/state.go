package models

import (
	"time"
)

// State constants represent the various states a Benthos instance can be in
const (
	// StateStopped indicates the instance is not running
	StateStopped = "stopped"
	// StateStarting indicates the instance is in the process of starting
	StateStarting = "starting"
	// StateRunning indicates the instance is running normally
	StateRunning = "running"
	// StateConfigChanged indicates a configuration change has been detected
	StateConfigChanged = "config_changed"
	// StateChangingConfig indicates the instance is applying a new configuration
	StateChangingConfig = "changing_config"
	// StateError indicates the instance has encountered an error
	StateError = "error"
)

// Event constants represent the events that can trigger state transitions
const (
	// EventStart is triggered to start an instance
	EventStart = "start"
	// EventStarted is triggered when an instance has successfully started
	EventStarted = "started"
	// EventStop is triggered to stop an instance
	EventStop = "stop"
	// EventStopped is triggered when an instance has successfully stopped
	EventStopped = "stopped"
	// EventConfigChange is triggered when a configuration change is detected
	EventConfigChange = "config_change"
	// EventUpdateConfig is triggered to update an instance's configuration
	EventUpdateConfig = "update_config"
	// EventUpdateDone is triggered when a configuration update is complete
	EventUpdateDone = "update_done"
	// EventFail is triggered when an error occurs
	EventFail = "fail"
	// EventRetry is triggered to retry after an error
	EventRetry = "retry"
	// EventShutdown is triggered to shut down the agent
	EventShutdown = "shutdown"
)

// Event represents a request to change the state of a Benthos instance
type Event struct {
	// InstanceID identifies the target instance
	InstanceID string
	// Type is the event type (one of the Event* constants)
	Type string
	// Payload contains additional data for the event (e.g., new configuration)
	Payload interface{}
	// Timestamp records when the event was created
	Timestamp time.Time
}

// NewEvent creates a new event with the current timestamp
func NewEvent(instanceID string, eventType string, payload interface{}) Event {
	return Event{
		InstanceID: instanceID,
		Type:       eventType,
		Payload:    payload,
		Timestamp:  time.Now(),
	}
}
