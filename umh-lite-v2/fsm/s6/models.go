package s6

import (
	"sync"

	"github.com/looplab/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/fsm/utils"
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

// State constants represent the various states a service can be in
const (
	// StateStarting indicates the service is in the process of starting
	StateStarting = "starting"
	// StateRunning indicates the service is running normally
	StateRunning = "running"
	// StateStopping indicates the service is in the process of stopping
	StateStopping = "stopping"
	// StateStopped indicates the service is not running
	StateStopped = "stopped"
	// StateFailed indicates the service has failed
	StateFailed = "failed"
	// StateUnknown indicates the service status is unknown
	StateUnknown = "unknown"
)

// Event constants represent the events that can trigger state transitions
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

// S6Instance represents a single S6 service instance with a state machine
type S6Instance struct {
	// ID is a unique identifier for this instance (service name)
	ID string

	// Mutex for protecting concurrent access to fields
	mu sync.RWMutex

	// FSM is the finite state machine that manages instance state
	FSM *fsm.FSM

	// DesiredState represents the target state we want to reach
	DesiredState string

	// CurrentState represents the current state of the instance
	CurrentState string

	// Callbacks for state transitions
	callbacks map[string]fsm.Callback

	// BackoffManager for managing retry attempts
	backoffManager *utils.TransitionBackoffManager

	// lastError stores the last error that occurred during a transition
	lastError error

	// ServicePath is the path to the s6 service directory
	ServicePath string

	// ExternalState contains all metrics, logs, etc.
	// that are updated at the beginning of Reconcile and then used to
	// determine the next state
	ExternalState ExternalState
}

// ExternalState contains all metrics, logs, etc.
// that are updated at the beginning of Reconcile and then used to
// determine the next state
type ExternalState struct {
	Status S6ServiceStatus
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
