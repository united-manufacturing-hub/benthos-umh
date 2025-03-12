package benthos

import (
	"context"
	"fmt"

	"github.com/looplab/fsm"
)

// NewBenthosInstance creates a new BenthosInstance with the given ID
func NewBenthosInstance(id string, callbacks map[string]fsm.Callback) *BenthosInstance {
	if callbacks == nil {
		callbacks = make(map[string]fsm.Callback)
	}

	instance := &BenthosInstance{
		ID:           id,
		DesiredState: StateStopped,
		callbacks:    callbacks,
	}

	// Define the FSM transitions
	instance.FSM = fsm.NewFSM(
		StateStopped, // Initial state
		fsm.Events{
			// Stopped -> Starting -> Running
			{Name: EventStart, Src: []string{StateStopped}, Dst: StateStarting},
			{Name: EventStartDone, Src: []string{StateStarting}, Dst: StateRunning},
			{Name: EventStop, Src: []string{StateRunning, StateStarting}, Dst: StateStopping},
			{Name: EventStopDone, Src: []string{StateStopping}, Dst: StateStopped},
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
	if state != StateRunning && state != StateStopped {
		return fmt.Errorf("invalid desired state: %s. valid states are %s and %s", state, StateRunning, StateStopped)
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
