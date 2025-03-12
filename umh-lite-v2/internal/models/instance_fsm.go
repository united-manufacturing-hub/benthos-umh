package models

import (
	"context"
	"sync"
	"time"

	"github.com/looplab/fsm"
)

// BenthosInstance represents a single Benthos pipeline instance
type BenthosInstance struct {
	// ID is a unique identifier for this instance
	ID string

	// Mutex for protecting concurrent access to fields
	mu sync.RWMutex

	// FSM is the finite state machine that manages instance state
	FSM *fsm.FSM

	// DesiredState represents the target state we want to reach
	DesiredState string

	// CurrentConfig is the currently applied configuration
	CurrentConfig *BenthosConfig
	// DesiredConfig is the new configuration to be applied
	DesiredConfig *BenthosConfig

	// LastTransition is the time of the last state transition
	LastTransition time.Time
	// LastError stores the most recent error, if any
	LastError error

	// RetryCount for transient errors
	RetryCount int
	// NextRetry is the time for the next retry attempt
	NextRetry time.Time

	// Process is the handle to the Benthos process (implementation-specific)
	// This could be a *exec.Cmd, a reference to a plugin instance, etc.
	Process interface{}

	// Callbacks for state transitions
	callbacks map[string]fsm.Callback
}

// NewBenthosInstance creates a new BenthosInstance with the given ID
func NewBenthosInstance(id string, callbacks map[string]fsm.Callback) *BenthosInstance {
	if callbacks == nil {
		callbacks = make(map[string]fsm.Callback)
	}

	instance := &BenthosInstance{
		ID:             id,
		DesiredState:   StateStopped,
		LastTransition: time.Now(),
		callbacks:      callbacks,
	}

	// Define the FSM transitions
	instance.FSM = fsm.NewFSM(
		StateStopped, // Initial state
		fsm.Events{
			// Stopped -> Starting -> Running
			{Name: EventStart, Src: []string{StateStopped, StateError}, Dst: StateStarting},
			{Name: EventStarted, Src: []string{StateStarting}, Dst: StateRunning},

			// Running -> Stopped
			{Name: EventStop, Src: []string{StateRunning, StateError}, Dst: StateStopped},

			// Config change flow
			{Name: EventConfigChange, Src: []string{StateRunning}, Dst: StateConfigChanged},
			{Name: EventUpdateConfig, Src: []string{StateConfigChanged}, Dst: StateChangingConfig},
			{Name: EventUpdateDone, Src: []string{StateChangingConfig}, Dst: StateStarting},

			// Error handling
			{Name: EventFail, Src: []string{StateStarting, StateRunning, StateChangingConfig}, Dst: StateError},
			{Name: EventRetry, Src: []string{StateError}, Dst: StateStopped},
		},
		fsm.Callbacks{
			"enter_state": func(ctx context.Context, e *fsm.Event) {
				instance.mu.Lock()
				instance.LastTransition = time.Now()
				instance.mu.Unlock()

				// Call registered callback for this state if exists
				if cb, ok := instance.callbacks["enter_"+e.Dst]; ok {
					cb(ctx, e)
				}
			},
		},
	)

	return instance
}

// GetState safely returns the current state
func (b *BenthosInstance) GetState() string {
	return b.FSM.Current()
}

// SetDesiredState safely updates the desired state
func (b *BenthosInstance) SetDesiredState(state string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.DesiredState = state
}

// GetDesiredState safely returns the desired state
func (b *BenthosInstance) GetDesiredState() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.DesiredState
}

// SetError records an error encountered by the instance
func (b *BenthosInstance) SetError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.LastError = err
	if err != nil {
		b.RetryCount++
		// Set a backoff time based on retry count (exponential backoff)
		backoff := time.Duration(1<<uint(b.RetryCount-1)) * time.Second
		if backoff > 1*time.Minute {
			backoff = 1 * time.Minute // Cap at 1 minute
		}
		b.NextRetry = time.Now().Add(backoff)
	}
}

// GetError safely returns the last error
func (b *BenthosInstance) GetError() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.LastError
}

// ClearError resets error state
func (b *BenthosInstance) ClearError() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.LastError = nil
	b.RetryCount = 0
}

// ShouldRetry returns true if it's time to retry after an error
func (b *BenthosInstance) ShouldRetry() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.LastError != nil && time.Now().After(b.NextRetry)
}

// SendEvent sends an event to the FSM and returns whether the event was processed
func (b *BenthosInstance) SendEvent(ctx context.Context, eventName string, args ...interface{}) error {
	return b.FSM.Event(ctx, eventName, args...)
}

// CanTransition checks if a transition is possible from the current state
func (b *BenthosInstance) CanTransition(eventName string) bool {
	return b.FSM.Can(eventName)
}
