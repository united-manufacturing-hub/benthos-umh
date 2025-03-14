package fsm

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/looplab/fsm"
	"go.uber.org/zap"
)

// BaseFSMInstance implements the public fsm.FSM interface
type BaseFSMInstance struct {
	cfg BaseFSMInstanceConfig

	// mu is a mutex for protecting concurrent access to fields
	mu sync.RWMutex

	// fsm is the finite state machine that manages instance state
	fsm *fsm.FSM

	// Callbacks for state transitions
	callbacks map[string]fsm.Callback

	// backoff is the backoff manager for managing retry attempts
	backoff backoff.BackOff

	// suspendedUntilTime is the time when the next reconcile is allowed
	// it is used in the reconcile function to check if the backoff has elapsed
	suspendedUntilTime time.Time

	// lastError stores the last error that occurred during a transition
	lastError error

	// logger is the logger for the FSM
	logger *zap.SugaredLogger
}

type BaseFSMInstanceConfig struct {
	ID              string
	DesiredFSMState string

	// FSM

	// OperationalStateAfterCreate is the operational state after the create event
	OperationalStateAfterCreate string
	// OperationalStateBeforeRemove is the operational state before the remove event
	// The lifecycle state removing is only allowed from this state
	OperationalStateBeforeRemove string
	// OperationalTransitions are the transitions that are allowed in the operational state
	OperationalTransitions []fsm.EventDesc
}

func NewBaseFSMInstance(cfg BaseFSMInstanceConfig, logger *zap.SugaredLogger) *BaseFSMInstance {

	baseInstance := &BaseFSMInstance{
		cfg:       cfg,
		callbacks: make(map[string]fsm.Callback),
		backoff: func() *backoff.ExponentialBackOff {
			b := backoff.NewExponentialBackOff()
			b.InitialInterval = 100 * time.Millisecond
			b.MaxInterval = 1 * time.Minute
			return b
		}(),
		logger: logger,
	}

	// Combine lifecycle and operational transitions
	events := []fsm.EventDesc{
		// Lifecycle transitions
		{Name: LifecycleEventCreate, Src: []string{LifecycleStateToBeCreated}, Dst: LifecycleStateCreating},
		{Name: LifecycleEventCreateDone, Src: []string{LifecycleStateCreating}, Dst: cfg.OperationalStateAfterCreate},
		{Name: LifecycleEventRemove, Src: []string{cfg.OperationalStateBeforeRemove}, Dst: LifecycleStateRemoving},
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
		baseInstance.logger.Debugf("Entering removed state for FSM %s", baseInstance.cfg.ID)
	})

	baseInstance.AddCallback("enter_"+LifecycleStateCreating, func(ctx context.Context, e *fsm.Event) {
		baseInstance.logger.Debugf("Entering creating state for FSM %s", baseInstance.cfg.ID)
	})

	baseInstance.AddCallback("enter_"+LifecycleStateToBeCreated, func(ctx context.Context, e *fsm.Event) {
		baseInstance.logger.Debugf("Entering to be created state for FSM %s", baseInstance.cfg.ID)
	})

	baseInstance.AddCallback("enter_"+LifecycleStateRemoving, func(ctx context.Context, e *fsm.Event) {
		baseInstance.logger.Debugf("Entering removing state for FSM %s", baseInstance.cfg.ID)
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

	// suspend the reconcile
	if s.suspendedUntilTime.IsZero() {
		next := s.backoff.NextBackOff()
		s.suspendedUntilTime = time.Now().Add(next)
		s.logger.Debugf("Suspending reconcile for %s because of error: %s", next, s.lastError)
	}
}

// setDesiredFSMState safely updates the desired state
// but does not check if the desired state is valid
func (s *BaseFSMInstance) SetDesiredFSMState(state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg.DesiredFSMState = state
}

// GetDesiredFSMState returns the desired state of the FSM
func (s *BaseFSMInstance) GetDesiredFSMState() string {
	return s.cfg.DesiredFSMState
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

// Remove starts the removal process, it is idempotent and can be called multiple times
// Note: it is only removed once IsRemoved returns true
func (s *BaseFSMInstance) Remove(ctx context.Context) error {
	// Set the desired state to the state before remove
	s.SetDesiredFSMState(s.cfg.OperationalStateBeforeRemove)
	return s.SendEvent(ctx, LifecycleEventRemove)
}

// IsRemoved returns true if the instance has been removed
func (s *BaseFSMInstance) IsRemoved() bool {
	return s.fsm.Current() == LifecycleStateRemoved
}

// ShouldSkipReconcileBecauseOfError returns true if the reconcile should be skipped because of an error
// that occurred in the last reconciliation and the backoff period has not yet elapsed
func (s *BaseFSMInstance) ShouldSkipReconcileBecauseOfError() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// If there is an error and the backoff period has not yet elapsed, skip the reconcile
	if s.lastError != nil && !s.suspendedUntilTime.IsZero() {
		if time.Now().Before(s.suspendedUntilTime) {
			s.logger.Debugf("Skipping reconcile because of error: %s. Remaining backoff: %s", s.lastError, time.Until(s.suspendedUntilTime))
			// It's still too early to retry
			return true
		}

		// Reset the suspendedUntilTime so that the next error can trigger a backoff increase again
		s.suspendedUntilTime = time.Time{}
	}

	return false
}

// ResetState clears the error and backoff after a successful reconcile
func (s *BaseFSMInstance) ResetState() {
	s.ClearError()
	s.backoff.Reset()
	s.suspendedUntilTime = time.Time{}
}

func (s *BaseFSMInstance) GetID() string {
	return s.cfg.ID
}

func (s *BaseFSMInstance) GetLogger() *zap.SugaredLogger {
	return s.logger
}
