package fsm

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/looplab/fsm"
)

// BaseFSMInstance implements the public fsm.FSM interface
type BaseFSMInstance struct {
	// ID is a unique identifier for this instance (service name)
	ID string

	// DesiredFSMState represents the target operational state we want to reach
	DesiredFSMState string

	// mu is a mutex for protecting concurrent access to fields
	mu sync.RWMutex

	// fsm is the finite state machine that manages instance state
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

type BaseFSMInstanceConfig struct {
	ID              string
	DesiredFSMState string

	// FSM

	// OperationalStateAfterCreate is the operational state after the create event
	OperationalStateAfterCreate string
	// OperationalStates are the operational states of the FSM,
	// these are the states that allow to be transitioned from into a removed state,
	// as we always want to be able to remove the instance
	OperationalStatesBeforeRemove []string
	// OperationalTransitions are the transitions that are allowed in the operational state
	OperationalTransitions []fsm.EventDesc
}

func NewBaseFSMInstance(cfg BaseFSMInstanceConfig) *BaseFSMInstance {

	baseInstance := &BaseFSMInstance{
		ID:              cfg.ID,
		DesiredFSMState: cfg.DesiredFSMState,
		callbacks:       make(map[string]fsm.Callback),
		backoff: func() *backoff.ExponentialBackOff {
			b := backoff.NewExponentialBackOff()
			b.InitialInterval = 100 * time.Millisecond
			b.MaxInterval = 1 * time.Minute
			return b
		}(),
	}

	// Combine lifecycle and operational transitions
	events := []fsm.EventDesc{
		// Lifecycle transitions
		{Name: LifecycleEventCreate, Src: []string{LifecycleStateToBeCreated}, Dst: LifecycleStateCreating},
		{Name: LifecycleEventCreateDone, Src: []string{LifecycleStateCreating}, Dst: cfg.OperationalStateAfterCreate},
		{Name: LifecycleEventRemove, Src: cfg.OperationalStatesBeforeRemove, Dst: LifecycleStateRemoving},
		{Name: LifecycleEventRemoveDone, Src: []string{LifecycleStateRemoving}, Dst: LifecycleStateRemoved},
	}
	events = append(events, cfg.OperationalTransitions...)

	//
	baseInstance.fsm = fsm.NewFSM(
		LifecycleStateToBeCreated,
		fsm.Events(events),
		fsm.Callbacks{
			"enter_state": func(ctx context.Context, e *fsm.Event) {
				// Call registered callback for this state if exists
				if cb, ok := baseInstance.callbacks["enter_"+e.Dst]; ok {
					cb(ctx, e)
				}
			},
		},
	)

	// Register default lifecycle callbacks

	baseInstance.AddCallback("enter_"+LifecycleStateRemoved, func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is removed", baseInstance.ID)
	})

	baseInstance.AddCallback("enter_"+LifecycleStateCreating, func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is creating", baseInstance.ID)
	})

	baseInstance.AddCallback("enter_"+LifecycleStateToBeCreated, func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is to be created", baseInstance.ID)
	})

	baseInstance.AddCallback("enter_"+LifecycleStateRemoving, func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is removing", baseInstance.ID)
	})

	return baseInstance
}

// AddCallback adds a callback for a given event name
func (s *BaseFSMInstance) AddCallback(eventName string, callback fsm.Callback) {
	s.callbacks[eventName] = callback
}

// GetError returns the last error that occurred during a transition
func (s *BaseFSMInstance) GetError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastError
}

// SetError sets the last error that occurred during a transition
func (s *BaseFSMInstance) SetError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastError = err
	s.suspendedTime = time.Now()
}

// setDesiredFSMState safely updates the desired state
// but does not check if the desired state is valid
func (s *BaseFSMInstance) SetDesiredFSMState(state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.DesiredFSMState = state
}

// GetDesiredFSMState returns the desired state of the FSM
func (s *BaseFSMInstance) GetDesiredFSMState() string {
	return s.DesiredFSMState
}

// GetCurrentFSMState returns the current state of the FSM
func (s *BaseFSMInstance) GetCurrentFSMState() string {
	return s.fsm.Current()
}

// SetCurrentFSMState sets the current state of the FSM
// This should only be called in tests
func (s *BaseFSMInstance) SetCurrentFSMState(state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fsm.SetState(state)
}

// SendEvent sends an event to the FSM and returns whether the event was processed
func (s *BaseFSMInstance) SendEvent(ctx context.Context, eventName string, args ...interface{}) error {
	return s.fsm.Event(ctx, eventName, args...)
}

// ClearError clears any error state and resets the backoff
func (s *BaseFSMInstance) ClearError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastError = nil
	s.backoff.Reset()
}

// Remove starts the removal process
// Note: it is only removed once IsRemoved returns true
func (s *BaseFSMInstance) Remove(ctx context.Context) error {
	return s.SendEvent(ctx, LifecycleEventRemove)
}

// IsRemoved returns true if the instance has been removed
func (s *BaseFSMInstance) IsRemoved() bool {
	return s.fsm.Current() == LifecycleStateRemoved
}

// ShouldSkipReconcileBecauseOfError returns true if the reconcile should be skipped because of an error
// that occurred in the last reconciliation and the backoff period has not yet elapsed
func (s *BaseFSMInstance) ShouldSkipReconcileBecauseOfError() bool {
	if s.lastError != nil {
		// Check how long we are supposed to wait
		next := s.backoff.NextBackOff() // e.g. 100ms, 200ms, 400ms...
		if time.Since(s.suspendedTime) < next {
			// It's still too early to retry
			return true
		}
	}

	return false
}

// ResetState clears the error and backoff after a successful reconcile
func (s *BaseFSMInstance) ResetState() {
	s.ClearError()
	s.backoff.Reset()
}
