package benthos

import (
	"context"
	"fmt"
)

// Reconcile examines the BenthosInstance and, in three steps:
//  1. Detect any external changes (e.g., a new configuration or external signals).
//  2. Check if a previous transition failed; if so, verify whether the backoff has elapsed.
//  3. Attempt the required state transition by sending the appropriate event.
//
// This function is intended to be called repeatedly (e.g. in a periodic control loop).
// Over multiple calls, it converges the actual state to the desired state. Transitions
// that fail are retried in subsequent reconcile calls after a backoff period.
func (b *BenthosInstance) Reconcile(ctx context.Context) error {
	// Step 1: Detect external changes.
	if err := b.reconcileExternalChanges(ctx); err != nil {
		return err
	}

	// Step 2: Check if a previous transition failed and if backoff has elapsed.
	if !b.reconcileBackoffElapsed() {
		// Too soon to retry; exit and try again in the next reconciliation cycle.
		return nil
	}

	// Step 3: Attempt to reconcile the state.
	return b.reconcileStateTransition(ctx)
}

// reconcileExternalChanges checks if the BenthosInstance has encountered new config
// or external triggers that necessitate a state change, sending the corresponding events.
func (b *BenthosInstance) reconcileExternalChanges(ctx context.Context) error {
	// Example: if we discover a changed config, we might:
	// if b.NeedsConfigChange() { ... send EventConfigChange or EventUpdateConfig ... }
	//
	// For now, it's a stub that always returns nil.
	return nil
}

// reconcileBackoffElapsed returns true if no transition error is recorded or if
// the required backoff period has elapsed since the last error. If false, we skip
// further transitions this iteration.
func (b *BenthosInstance) reconcileBackoffElapsed() bool {
	if err := b.GetError(); err != nil {
		return b.backoffManager.ReconcileBackoffElapsed()
	}
	return true
}

// reconcileStateTransition compares the current state with the desired state
// and, if necessary, sends events to drive the FSM from the current to the desired state.
func (b *BenthosInstance) reconcileStateTransition(ctx context.Context) error {
	currentState := b.GetState()
	desiredState := b.GetDesiredState()

	// If already in the desired state, nothing to do.
	if currentState == desiredState {
		return nil
	}

	switch desiredState {
	case StateRunning:
		return b.reconcileTransitionToRunning(ctx, currentState)
	case StateStopped:
		return b.reconcileTransitionToStopped(ctx, currentState)
	default:
		return fmt.Errorf("invalid desired state: %s", desiredState)
	}

	return nil
}

// reconcileTransitionToRunning handles transitions when the desired state is Running.
// It deals with moving from Stopped to Starting and then to Running.
func (b *BenthosInstance) reconcileTransitionToRunning(ctx context.Context, currentState string) error {
	if currentState == StateStopped {
		// Attempt to initiate start (e.g. perform necessary pre-start checks).
		if err := b.InitiateBenthosStart(ctx); err != nil {
			return err
		}
		// Send event to transition from Stopped to Starting.
		return b.sendEvent(ctx, EventStart)
	}

	if currentState == StateStarting {
		// If already in the process of starting, check if the instance is healthy.
		if b.IsBenthosRunning() {
			// Transition from Starting to Running.
			return b.sendEvent(ctx, EventStartDone)
		}
		// Otherwise, wait for the next reconcile cycle.
		return nil
	}

	return nil
}

// reconcileTransitionToStopped handles transitions when the desired state is Stopped.
// It deals with moving from Running (or Starting) to Stopping and then to Stopped.
func (b *BenthosInstance) reconcileTransitionToStopped(ctx context.Context, currentState string) error {
	if currentState == StateRunning || currentState == StateStarting {
		// Attempt to initiate a stop (e.g. perform cleanup).
		if err := b.InitiateBenthosStop(ctx); err != nil {
			return err
		}
		// Send event to transition to Stopping.
		return b.sendEvent(ctx, EventStop)
	}

	if currentState == StateStopping {
		// If already stopping, verify if the instance is completely stopped.
		if b.IsBenthosStopped() {
			// Transition from Stopping to Stopped.
			return b.sendEvent(ctx, EventStopDone)
		}
		return nil
	}

	return nil
}
