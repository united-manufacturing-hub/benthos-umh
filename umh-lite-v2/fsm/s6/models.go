package s6

import (
	"sync"
	"time"

	"github.com/looplab/fsm"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/service/s6"

	"github.com/cenkalti/backoff/v4"
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
	// Uptime is the service uptime in seconds (if running)
	Uptime int64
	// Pid is the process ID of the service (if running)
	Pid int
}

// S6Instance represents a single S6 service instance with a state machine
type S6Instance struct {
	// ID is a unique identifier for this instance (service name)
	ID string

	// Mutex for protecting concurrent access to fields
	mu sync.RWMutex

	// FSM is the finite state machine that manages instance state
	FSM *fsm.FSM

	// DesiredFSMState represents the target operational state we want to reach
	DesiredFSMState string

	// Callbacks for state transitions
	callbacks map[string]fsm.Callback

	// backoff is the backoff manager for managing retry attempts
	backoff backoff.BackOff

	// suspendedTime is the time when the last error occurred
	// it is used in the reconcile function to check if the backoff has elapsed
	suspendedTime time.Time

	// lastError stores the last error that occurred during a transition
	lastError error

	// ServicePath is the path to the s6 service directory
	ServicePath string

	// ObservedState represents the observed state of the service
	// ObservedState contains all metrics, logs, etc.
	// that are updated at the beginning of Reconcile and then used to
	// determine the next state
	ObservedState S6ObservedState

	// service is the S6 service implementation to use
	service s6service.Service
}

// GetError returns the last error that occurred during a transition
func (s *S6Instance) GetError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastError
}

// SetError sets the last error that occurred during a transition
func (s *S6Instance) SetError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastError = err
}
