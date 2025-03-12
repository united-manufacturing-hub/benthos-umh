package s6

import (
	"context"
	"fmt"
)

// Reconcile examines the S6Instance and, in three steps:
//  1. Detect any external changes (e.g., service crashed or was manually restarted).
//  2. Check if a previous transition failed; if so, verify whether the backoff has elapsed.
//  3. Attempt the required state transition by sending the appropriate event.
//
// This function is intended to be called repeatedly (e.g. in a periodic control loop).
// Over multiple calls, it converges the actual state to the desired state. Transitions
// that fail are retried in subsequent reconcile calls after a backoff period.
func (s *S6Instance) Reconcile(ctx context.Context) error {
	// Step 1: Detect external changes.
	if err := s.reconcileExternalChanges(ctx); err != nil {
		return err
	}

	// Step 2: Check if a previous transition failed and if backoff has elapsed.
	if !s.reconcileBackoffElapsed() {
		// Too soon to retry; exit and try again in the next reconciliation cycle.
		return nil
	}

	// Step 3: Attempt to reconcile the state.
	return s.reconcileStateTransition(ctx)
}

// reconcileExternalChanges checks if the S6Instance service status has changed
// externally (e.g., if someone manually stopped or started it, or if it crashed)
func (s *S6Instance) reconcileExternalChanges(ctx context.Context) error {
	// Get the current status of the S6 service
	status, err := s.getS6Status(ctx)
	if err != nil {
		return fmt.Errorf("failed to get S6 status: %w", err)
	}

	// Update the external state with the new status
	s.ExternalState.Status = status

	return nil
}

// reconcileBackoffElapsed returns true if no transition error is recorded or if
// the required backoff period has elapsed since the last error. If false, we skip
// further transitions this iteration.
func (s *S6Instance) reconcileBackoffElapsed() bool {
	if err := s.GetError(); err != nil {
		return s.backoffManager.ReconcileBackoffElapsed()
	}
	return true
}

// reconcileStateTransition compares the current state with the desired state
// and, if necessary, sends events to drive the FSM from the current to the desired state.
func (s *S6Instance) reconcileStateTransition(ctx context.Context) error {
	currentState := s.GetState()
	desiredState := s.GetDesiredState()

	// If already in the desired state, nothing to do.
	if currentState == desiredState {
		return nil
	}

	switch desiredState {
	case StateRunning:
		return s.reconcileTransitionToRunning(ctx, currentState)
	case StateStopped:
		return s.reconcileTransitionToStopped(ctx, currentState)
	default:
		return fmt.Errorf("invalid desired state: %s", desiredState)
	}
}

// reconcileTransitionToRunning handles transitions when the desired state is Running.
// It deals with moving from Stopped/Failed to Starting and then to Running.
func (s *S6Instance) reconcileTransitionToRunning(ctx context.Context, currentState string) error {
	if currentState == StateStopped || currentState == StateFailed {
		// Attempt to initiate start
		if err := s.InitiateS6Start(ctx); err != nil {
			return err
		}
		// Send event to transition from Stopped/Failed to Starting
		return s.sendEvent(ctx, EventStart)
	}

	if currentState == StateStarting {
		// If already in the process of starting, check if the service is healthy
		if s.IsS6Running() {
			// Transition from Starting to Running
			return s.sendEvent(ctx, EventStartDone)
		}
		// Otherwise, wait for the next reconcile cycle
		return nil
	}

	return nil
}

// reconcileTransitionToStopped handles transitions when the desired state is Stopped.
// It deals with moving from Running/Starting/Failed to Stopping and then to Stopped.
func (s *S6Instance) reconcileTransitionToStopped(ctx context.Context, currentState string) error {
	if currentState == StateRunning || currentState == StateStarting {
		// Attempt to initiate a stop
		if err := s.InitiateS6Stop(ctx); err != nil {
			return err
		}
		// Send event to transition to Stopping
		return s.sendEvent(ctx, EventStop)
	}

	if currentState == StateStopping {
		// If already stopping, verify if the instance is completely stopped
		if s.IsS6Stopped() {
			// Transition from Stopping to Stopped
			return s.sendEvent(ctx, EventStopDone)
		}
		return nil
	}

	if currentState == StateFailed {
		// If in failed state, transition directly to stopped since service is already down
		// This assumes that a failed service is not running
		return s.sendEvent(ctx, EventStopDone)
	}

	return nil
}
