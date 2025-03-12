package benthos

import (
	"sync"

	"github.com/looplab/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/fsm/utils"
)

// LifecycleState constants represent the various lifecycle states a Benthos instance can be in
// They will be handled before the operational states
const (
	// LifecycleStateToBeCreated indicates the instance has not been created yet
	LifecycleStateToBeCreated = "to_be_created"
	// LifecycleStateCreating indicates the instance is being created
	LifecycleStateCreating = "creating"
	// LifecycleStateRemoving indicates the instance is being removed
	LifecycleStateRemoving = "removing"
	// LifecycleStateRemoved indicates the instance has been removed and can be cleaned up
	LifecycleStateRemoved = "removed"
)

// State type checks
func IsLifecycleState(state string) bool {
	switch state {
	case LifecycleStateToBeCreated,
		LifecycleStateCreating,
		LifecycleStateRemoving,
		LifecycleStateRemoved:
		return true
	default:
		return false
	}
}

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

	// EventRemove is triggered to remove an instance
	EventRemove = "remove"
	// EventRemoveDone is triggered when the instance has been removed
	EventRemoveDone = "remove_done"
	// EventCreate is triggered to create an instance
	EventCreate = "create"
	// EventCreateDone is triggered when the instance has been created
	EventCreateDone = "create_done"
)

// BenthosInstance represents a single Benthos pipeline instance with a state machine
type BenthosInstance struct {
	// ID is a unique identifier for this instance
	ID string

	// Mutex for protecting concurrent access to fields
	mu sync.RWMutex

	// FSM is the finite state machine that manages instance state
	FSM *fsm.FSM

	// DesiredState represents the target state we want to reach
	DesiredState string

	// CurrentState represents the current state of the instance
	CurrentState string

	// S6FSM is the FSM of the S6 service
	S6FSM interface{}

	// Callbacks for state transitions
	callbacks map[string]fsm.Callback

	// BackoffManager for managing retry attempts
	backoffManager *utils.TransitionBackoffManager

	// lastError stores the last error that occurred during a transition
	lastError error

	// ExternalState contains all metrics, logs, etc.
	// that are updated at the beginning of Reconcile and then used to
	// determine the next state
	ExternalState ExternalState
}

// ExternalState contains all metrics, logs, etc.
// that are updated at the beginning of Reconcile and then used to
// determine the next state
type ExternalState struct {
	IsRunning bool
}

// GetError returns the last error that occurred during a transition
func (b *BenthosInstance) GetError() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.lastError
}

// SetError sets the last error that occurred during a transition
func (b *BenthosInstance) SetError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastError = err
}
