package benthos

import (
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/looplab/fsm"
)

// OperationalState constants represent the various operational states a Benthos instance can be in
// They will be handled after the lifecycle states
const (
	// OperationalStateStarting indicates the instance is in the process of starting
	OperationalStateStarting = "starting"
	// OperationalStateRunning indicates the instance is running normally
	OperationalStateRunning = "running"
	// OperationalStateStopping indicates the instance is in the process of stopping
	OperationalStateStopping = "stopping"
	// OperationalStateStopped indicates the instance is not running
	OperationalStateStopped = "stopped"
)

func IsOperationalState(state string) bool {
	switch state {
	case OperationalStateStopped,
		OperationalStateStarting,
		OperationalStateRunning,
		OperationalStateStopping:
		return true
	default:
		return false
	}
}

// Event constants represent the events that can trigger state transitions
const (
	// EventStart is triggered to start an instance
	EventStart = "start"
	// EventStartDone is triggered when the instance has started
	EventStartDone = "start_done"
	// EventStop is triggered to stop an instance
	EventStop = "stop"
	// EventStopDone is triggered when the instance has stopped
	EventStopDone = "stop_done"
)

// BenthosInstance represents a single Benthos pipeline instance with a state machine
type BenthosInstance struct {
	// ID is a unique identifier for this instance
	ID string

	// DesiredFSMState represents the target state we want to reach
	DesiredFSMState string

	// ObservedState contains all metrics, logs, etc.
	// that are updated at the beginning of Reconcile and then used to
	// determine the next state
	ObservedState ObservedState

	// Mutex for protecting concurrent access to fields
	mu sync.RWMutex

	// FSM is the finite state machine that manages instance state
	fsm *fsm.FSM

	// Callbacks for state transitions
	callbacks map[string]fsm.Callback

	// backoff is the backoff manager for managing retry attempts
	backoff backoff.BackOff

	// suspendedTime is the time when the last error occurred
	// it is used in the reconcile function to check if the backoff has elapsed
	suspendedTime time.Time

	// lastError stores the last error that occurred during a transition
	lastError error
}

// ExternalState contains all metrics, logs, etc.
// that are updated at the beginning of Reconcile and then used to
// determine the next state
type ObservedState struct {
	IsRunning bool
}

// GetError returns the last error that occurred during a transition
func (b *BenthosInstance) getError() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.lastError
}

// SetError sets the last error that occurred during a transition
func (b *BenthosInstance) setError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastError = err
}
