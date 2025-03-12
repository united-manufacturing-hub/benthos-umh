package s6

import (
	"context"
	"fmt"

	"github.com/looplab/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/fsm/utils"
)

// NewS6Instance creates a new S6Instance with the given ID and service path
func NewS6Instance(id string, servicePath string, callbacks map[string]fsm.Callback) *S6Instance {
	if callbacks == nil {
		callbacks = make(map[string]fsm.Callback)
	}

	instance := &S6Instance{
		ID:             id,
		DesiredState:   OperationalStateStopped,
		callbacks:      callbacks,
		ServicePath:    servicePath,
		backoffManager: utils.NewTransitionBackoffManager(),
		ExternalState:  S6ExternalState{Status: S6ServiceUnknown},
	}

	// Define the FSM transitions
	instance.FSM = fsm.NewFSM(
		utils.LifecycleStateToBeCreated, // Initial state is lifecycle state
		fsm.Events{
			// Lifecycle transitions
			{Name: utils.LifecycleEventCreate, Src: []string{utils.LifecycleStateToBeCreated}, Dst: utils.LifecycleStateCreating},
			{Name: utils.LifecycleEventCreateDone, Src: []string{utils.LifecycleStateCreating}, Dst: OperationalStateStopped},
			{Name: utils.LifecycleEventRemove, Src: []string{OperationalStateStopped}, Dst: utils.LifecycleStateRemoving},
			{Name: utils.LifecycleEventRemoveDone, Src: []string{utils.LifecycleStateRemoving}, Dst: utils.LifecycleStateRemoved},

			// Operational transitions (only valid when lifecycle state is "created")
			{Name: EventStart, Src: []string{OperationalStateStopped}, Dst: OperationalStateStarting},
			{Name: EventStartDone, Src: []string{OperationalStateStarting}, Dst: OperationalStateRunning},

			// Running/Starting -> Stopping -> Stopped
			{Name: EventStop, Src: []string{OperationalStateRunning, OperationalStateStarting}, Dst: OperationalStateStopping},
			{Name: EventStopDone, Src: []string{OperationalStateStopping}, Dst: OperationalStateStopped},

			// Restart from any state
			{Name: EventRestart, Src: []string{OperationalStateRunning, OperationalStateStopped}, Dst: OperationalStateStarting},

			// Lifecycle transitions
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
func (s *S6Instance) GetState() string {
	return s.FSM.Current()
}

// setDesiredState safely updates the desired state
// but does not check if the desired state is valid
func (s *S6Instance) setDesiredState(state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.DesiredState = state
}

// SetDesiredState safely updates the desired state
// But ensures that the desired state is a valid state and that it is also a reasonable state
// e.g., nobody wants to have an instance in the "starting" state, that is just intermediate
func (s *S6Instance) SetDesiredState(state string) error {
	if state != OperationalStateRunning && state != OperationalStateStopped {
		return fmt.Errorf("invalid desired state: %s. valid states are %s and %s", state, OperationalStateRunning, OperationalStateStopped)
	}

	s.setDesiredState(state)
	return nil
}

// GetDesiredState safely returns the desired state
func (s *S6Instance) GetDesiredState() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.DesiredState
}

// SendEvent sends an event to the FSM and returns whether the event was processed
func (s *S6Instance) sendEvent(ctx context.Context, eventName string, args ...interface{}) error {
	return s.FSM.Event(ctx, eventName, args...)
}
