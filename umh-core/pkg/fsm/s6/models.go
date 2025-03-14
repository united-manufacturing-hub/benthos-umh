package s6

import (
	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	public_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/s6"
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
	// LastStateChange is the timestamp of the last observed state change
	LastStateChange int64
	// ServiceInfo contains the actual service info from s6
	ServiceInfo s6service.ServiceInfo

	// ObservedS6ServiceConfig contains the actual service config from s6
	ObservedS6ServiceConfig s6service.S6ServiceConfig
}

// S6Instance implements the FSMInstance interface
// If S6Instance does not implement the FSMInstance interface, this will
// be detected at compile time
var _ public_fsm.FSMInstance = (*S6Instance)(nil)

// S6Instance represents a single S6 service instance with a state machine
type S6Instance struct {
	baseFSMInstance *internal_fsm.BaseFSMInstance

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
	config config.S6FSMConfig
}
