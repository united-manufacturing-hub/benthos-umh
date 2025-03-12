package s6

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/looplab/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/fsm/utils"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/service/s6"
)

// NewS6Instance creates a new S6Instance with the given ID and service path
func NewS6Instance(id string, servicePath string, callbacks map[string]fsm.Callback) *S6Instance {
	if callbacks == nil {
		callbacks = make(map[string]fsm.Callback)
	}

	instance := &S6Instance{
		ID:              id,
		DesiredFSMState: OperationalStateStopped,
		callbacks:       callbacks,
		ServicePath:     servicePath,
		backoff: func() *backoff.ExponentialBackOff {
			b := backoff.NewExponentialBackOff()
			b.InitialInterval = 100 * time.Millisecond
			b.MaxInterval = 1 * time.Minute
			return b
		}(),
		ObservedState: S6ObservedState{Status: S6ServiceUnknown},
		service:       s6service.NewDefaultService(),
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

// NewS6InstanceWithService creates a new S6Instance with a custom service implementation
func NewS6InstanceWithService(id string, servicePath string, service s6service.Service, callbacks map[string]fsm.Callback) *S6Instance {
	instance := NewS6Instance(id, servicePath, callbacks)
	instance.service = service
	return instance
}

// GetCurrentFSMState safely returns the current state
func (s *S6Instance) GetCurrentFSMState() string {
	return s.FSM.Current()
}

// setDesiredFSMState safely updates the desired state
// but does not check if the desired state is valid
func (s *S6Instance) setDesiredFSMState(state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.DesiredFSMState = state
}

// SetDesiredFSMState safely updates the desired state
// But ensures that the desired state is a valid state and that it is also a reasonable state
// e.g., nobody wants to have an instance in the "starting" state, that is just intermediate
func (s *S6Instance) SetDesiredFSMState(state string) error {
	if state != OperationalStateRunning && state != OperationalStateStopped {
		return fmt.Errorf("invalid desired state: %s. valid states are %s and %s", state, OperationalStateRunning, OperationalStateStopped)
	}

	s.setDesiredFSMState(state)
	return nil
}

// GetDesiredFSMState safely returns the desired state
func (s *S6Instance) GetDesiredFSMState() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.DesiredFSMState
}

// SendEvent sends an event to the FSM and returns whether the event was processed
func (s *S6Instance) sendEvent(ctx context.Context, eventName string, args ...interface{}) error {
	return s.FSM.Event(ctx, eventName, args...)
}

// ClearError clears any error state and resets the backoff
func (s *S6Instance) ClearError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastError = nil
	s.backoff.Reset()
}
