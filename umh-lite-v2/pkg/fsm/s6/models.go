package s6

import (
	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/fsm"
	public_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/pkg/fsm"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/pkg/service/s6"
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
	// S6ServiceUnknown indicates the service status is unknown
	S6ServiceUnknown S6ServiceStatus = "unknown"
)

// Operational state constants represent the runtime states of a service
const (
	// OperationalStateStarting indicates the service is in the process of starting
	OperationalStateStarting = "starting"
	// OperationalStateRunning indicates the service is running normally
	OperationalStateRunning = "running"
	// OperationalStateStopping indicates the service is in the process of stopping
	OperationalStateStopping = "stopping"
	// OperationalStateStopped indicates the service is not running
	OperationalStateStopped = "stopped"
	// OperationalStateUnknown indicates the service status is unknown
	OperationalStateUnknown = "unknown"
)

func IsOperationalState(state string) bool {
	switch state {
	case OperationalStateStopped,
		OperationalStateStarting,
		OperationalStateRunning,
		OperationalStateStopping,
		OperationalStateUnknown:
		return true
	default:
		return false
	}
}

// Operational event constants represent events related to service runtime states
const (
	// EventStart is triggered to start a service
	EventStart = "start"
	// EventStartDone is triggered when the service has started
	EventStartDone = "start_done"
	// EventStop is triggered to stop a service
	EventStop = "stop"
	// EventStopDone is triggered when the service has stopped
	EventStopDone = "stop_done"
	// EventFail is triggered when the service has failed
	EventFail = "fail"
	// EventRestart is triggered to restart a service
	EventRestart = "restart"
)

// S6ObservedState represents the state of the service as observed externally
type S6ObservedState struct {
	// Status represents the current observed status of the service
	Status S6ServiceStatus
	// LastStateChange is the timestamp of the last observed state change
	LastStateChange int64
	// ServiceInfo contains the actual service info from s6
	ServiceInfo s6service.ServiceInfo
}

// S6Instance represents a single S6 service instance with a state machine
type S6Instance struct {
	baseFSMInstance *internal_fsm.BaseFSMInstance
	public_fsm.FSMInstance

	// ObservedState represents the observed state of the service
	// ObservedState contains all metrics, logs, etc.
	// that are updated at the beginning of Reconcile and then used to
	// determine the next state
	ObservedState S6ObservedState

	// servicePath is the path to the s6 service directory
	servicePath string

	// service is the S6 service implementation to use
	service s6service.Service

	// config contains all the configuration for this service
	config s6service.ServiceConfig
}
