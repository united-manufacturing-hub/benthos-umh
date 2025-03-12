package benthos

import (
	"context"
	"fmt"

	"github.com/looplab/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/fsm/utils"
)

// TODO: states for creating and removing

// NewBenthosInstance creates a new BenthosInstance with the given ID
func NewBenthosInstance(id string, callbacks map[string]fsm.Callback) *BenthosInstance {
	if callbacks == nil {
		callbacks = make(map[string]fsm.Callback)
	}

	instance := &BenthosInstance{
		ID:           id,
		DesiredState: OperationalStateStopped,
		callbacks:    callbacks,
		ExternalState: ExternalState{
			IsRunning: false,
		},
		backoffManager: utils.NewTransitionBackoffManager(),
	}

	// Define the FSM transitions
	instance.FSM = fsm.NewFSM(
		LifecycleStateToBeCreated, // Initial state
		fsm.Events{
			// StateToBeCreated -> Creating -> Stopped
			{Name: EventCreate, Src: []string{LifecycleStateToBeCreated}, Dst: LifecycleStateCreating},
			{Name: EventCreateDone, Src: []string{LifecycleStateCreating}, Dst: OperationalStateStopped},

			// Stopped -> Starting -> Running
			{Name: EventStart, Src: []string{OperationalStateStopped}, Dst: OperationalStateStarting},
			{Name: EventStartDone, Src: []string{OperationalStateStarting}, Dst: OperationalStateRunning},
			{Name: EventStop, Src: []string{OperationalStateRunning, OperationalStateStarting}, Dst: OperationalStateStopping},
			{Name: EventStopDone, Src: []string{OperationalStateStopping}, Dst: OperationalStateStopped},

			// Starting / Running / Stopping / Stopped -> Removing
			{Name: EventRemove, Src: []string{OperationalStateStarting, OperationalStateRunning, OperationalStateStopping, OperationalStateStopped}, Dst: LifecycleStateRemoving},
			{Name: EventRemoveDone, Src: []string{LifecycleStateRemoving}, Dst: LifecycleStateRemoved},
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

	return instance
}

func (b *BenthosInstance) Remove(ctx context.Context) error {
	return b.sendEvent(ctx, EventRemove)
}

// GetState safely returns the current state
func (b *BenthosInstance) GetState() string {
	return b.FSM.Current()
}

// setDesiredState safely updates the desired state
// but does not check if the desired state is valid
func (b *BenthosInstance) setDesiredState(state string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.DesiredState = state
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
	return b.DesiredState
}

// SendEvent sends an event to the FSM and returns whether the event was processed
func (b *BenthosInstance) sendEvent(ctx context.Context, eventName string, args ...interface{}) error {
	return b.FSM.Event(ctx, eventName, args...)
}

// SetExternalState updates the external running state of the instance
// This should only be used by tests
func (b *BenthosInstance) SetExternalState(isRunning bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ExternalState.IsRunning = isRunning
}

// ClearError clears any error state and resets the backoff
func (b *BenthosInstance) ClearError() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastError = nil
	if b.backoffManager != nil {
		b.backoffManager.Reset()
	}
}
