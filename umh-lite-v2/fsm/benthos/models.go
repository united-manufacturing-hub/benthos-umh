package benthos

import (
	"sync"

	"github.com/looplab/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/fsm/utils"
)

// State constants represent the various states a Benthos instance can be in
const (
	// StateStarting indicates the instance is in the process of starting
	StateStarting = "starting"
	// StateRunning indicates the instance is running normally
	StateRunning = "running"
	// StateStopping indicates the instance is in the process of stopping
	StateStopping = "stopping"
	// StateStopped indicates the instance is not running
	StateStopped = "stopped"
)

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
