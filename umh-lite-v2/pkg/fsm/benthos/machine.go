package benthos

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/looplab/fsm"

	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/fsm/utils"
)

// NewBenthosInstance creates a new BenthosInstance with the given ID
func NewBenthosInstance(id string, callbacks map[string]fsm.Callback) *BenthosInstance {
	if callbacks == nil {
		callbacks = make(map[string]fsm.Callback)
	}

	instance := &BenthosInstance{
		ID:              id,
		DesiredFSMState: OperationalStateStopped,
		callbacks:       callbacks,
		ObservedState: ObservedState{
			IsRunning: false,
		},
		backoff: func() *backoff.ExponentialBackOff {
			b := backoff.NewExponentialBackOff()
			b.InitialInterval = 100 * time.Millisecond
			b.MaxInterval = 1 * time.Minute
			return b
		}(),
	}

	// Define the FSM transitions
	instance.fsm = fsm.NewFSM(
		utils.LifecycleStateToBeCreated, // Initial state
		fsm.Events{
			// Lifecycle transitions
			{Name: utils.LifecycleEventCreate, Src: []string{utils.LifecycleStateToBeCreated}, Dst: utils.LifecycleStateCreating},
			{Name: utils.LifecycleEventCreateDone, Src: []string{utils.LifecycleStateCreating}, Dst: OperationalStateStopped},
			{Name: utils.LifecycleEventRemove, Src: []string{OperationalStateStarting, OperationalStateRunning, OperationalStateStopping, OperationalStateStopped}, Dst: utils.LifecycleStateRemoving},
			{Name: utils.LifecycleEventRemoveDone, Src: []string{utils.LifecycleStateRemoving}, Dst: utils.LifecycleStateRemoved},

			// Operational transitions
			{Name: EventStart, Src: []string{OperationalStateStopped}, Dst: OperationalStateStarting},
			{Name: EventStartDone, Src: []string{OperationalStateStarting}, Dst: OperationalStateRunning},
			{Name: EventStop, Src: []string{OperationalStateRunning, OperationalStateStarting}, Dst: OperationalStateStopping},
			{Name: EventStopDone, Src: []string{OperationalStateStopping}, Dst: OperationalStateStopped},
		},
		fsm.Callbacks{
			"enter_state": func(ctx context.Context, e *fsm.Event) {

				// Call registered callback for this state if exists
				if cb, ok := instance.callbacks["enter_"+e.Dst]; ok {
					cb(ctx, e)
				}
			},
		},
	)

	instance.registerCallbacks()

	return instance
}

func (b *BenthosInstance) Remove(ctx context.Context) error {
	return b.sendEvent(ctx, utils.LifecycleEventRemove)
}

// GetState safely returns the current state
func (b *BenthosInstance) GetState() string {
	return b.fsm.Current()
}

// setDesiredState safely updates the desired state
// but does not check if the desired state is valid
func (b *BenthosInstance) setDesiredState(state string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.DesiredFSMState = state
}

// SetDesiredState safely updates the desired state
// But ensures that the desired state is a valid state and that is is also a reasonable state
// e.g., nobody wants to have an instance in the "starting" state, that is just intermediate
func (b *BenthosInstance) SetDesiredState(state string) error {
	if state != OperationalStateRunning && state != OperationalStateStopped {
		return fmt.Errorf("invalid desired state: %s. valid states are %s and %s", state, OperationalStateRunning, OperationalStateStopped)
	}

	b.setDesiredState(state)
	return nil
}

// GetDesiredState safely returns the desired state
func (b *BenthosInstance) GetDesiredState() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.DesiredFSMState
}

// SendEvent sends an event to the FSM and returns whether the event was processed
func (b *BenthosInstance) sendEvent(ctx context.Context, eventName string, args ...interface{}) error {
	return b.fsm.Event(ctx, eventName, args...)
}

// SetObservedState updates the external running state of the instance
// This should only be used by tests
func (b *BenthosInstance) setObservedState(isRunning bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ObservedState.IsRunning = isRunning
}

// ClearError clears any error state and resets the backoff
func (b *BenthosInstance) clearError() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastError = nil
	b.backoff.Reset()
}
