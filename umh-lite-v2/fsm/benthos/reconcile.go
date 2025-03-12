package benthos

import (
	"context"
	"fmt"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/fsm/utils"
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
	if b.lastError != nil {
		// Check how long we are supposed to wait
		next := b.backoff.NextBackOff() // e.g. 100ms, 200ms, 400ms...
		timeSinceSuspended := time.Since(b.suspendedTime)
		if timeSinceSuspended < next {
			// It's still too early to retry
			return nil
		}
	}

	// Step 3: Attempt to reconcile the state.
	err := b.reconcileStateTransition(ctx)
	if err != nil {
		b.SetError(err)
		b.suspendedTime = time.Now()
		return err
	}
	return nil
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

// reconcileStateTransition compares the current state with the desired state
// and, if necessary, sends events to drive the FSM from the current to the desired state.
// Any functions that fetch information are disallowed here and must be called in reconcileExternalChanges
// and exist in BenthosInstanceExternalState.
// This is to ensure full testability of the FSM.
func (b *BenthosInstance) reconcileStateTransition(ctx context.Context) error {
	currentState := b.GetState()
	desiredState := b.GetDesiredState()

	// If already in the desired state, nothing to do.
	if currentState == desiredState {
		return nil
	}

	// Handle lifecycle states first - these take precedence over operational states
	if utils.IsLifecycleState(currentState) {
		return b.reconcileLifecycleStates(ctx, currentState)
	}

	// Handle operational states
	if IsOperationalState(currentState) {
		return b.reconcileOperationalStates(ctx, currentState, desiredState)
	}

	return fmt.Errorf("invalid state: %s", currentState)
}

// reconcileLifecycleStates handles states related to instance lifecycle (creating/removing)
func (b *BenthosInstance) reconcileLifecycleStates(ctx context.Context, currentState string) error {
	// Independent what the desired state is, we always need to reconcile the lifecycle states first
	switch currentState {
	case utils.LifecycleStateToBeCreated:
		if err := b.InitiateServiceCreation(ctx); err != nil {
			return err
		}
		return b.sendEvent(ctx, utils.LifecycleEventCreate)
	case utils.LifecycleStateCreating:
		// TODO: check if the service is created
		return b.sendEvent(ctx, utils.LifecycleEventCreateDone)
	case utils.LifecycleStateRemoving:
		if err := b.InitiateServiceRemoval(ctx); err != nil {
			return err
		}
		return b.sendEvent(ctx, utils.LifecycleEventRemoveDone)
	case utils.LifecycleStateRemoved:
		return fmt.Errorf("instance %s is removed", b.ID)
	default:
		// If we are not in a lifecycle state, just continue
		return nil
	}
}

// reconcileOperationalStates handles states related to instance operations (starting/stopping)
func (b *BenthosInstance) reconcileOperationalStates(ctx context.Context, currentState string, desiredState string) error {
	switch desiredState {
	case OperationalStateRunning:
		return b.reconcileTransitionToRunning(ctx, currentState)
	case OperationalStateStopped:
		return b.reconcileTransitionToStopped(ctx, currentState)
	default:
		return fmt.Errorf("invalid desired state: %s", desiredState)
	}
}

// reconcileTransitionToRunning handles transitions when the desired state is Running.
// It deals with moving from Stopped to Starting and then to Running.
func (b *BenthosInstance) reconcileTransitionToRunning(ctx context.Context, currentState string) error {

	switch currentState {
	case OperationalStateStopped:
		// Attempt to initiate start (e.g. perform necessary pre-start checks).
		if err := b.InitiateBenthosStart(ctx); err != nil {
			return err
		}
		// Send event to transition from Stopped to Starting.
		return b.sendEvent(ctx, EventStart)
	case OperationalStateStarting:
		// If already in the process of starting, check if the instance is healthy.
		if b.IsBenthosRunning() {
			// Transition from Starting to Running.
			return b.sendEvent(ctx, EventStartDone)
		}
		// Otherwise, wait for the next reconcile cycle.
		return nil
	default:
		return fmt.Errorf("invalid current state: %s", currentState)
	}
}

// reconcileTransitionToStopped handles transitions when the desired state is Stopped.
// It deals with moving from Running (or Starting) to Stopping and then to Stopped.
func (b *BenthosInstance) reconcileTransitionToStopped(ctx context.Context, currentState string) error {

	switch currentState {
	case OperationalStateRunning, OperationalStateStarting:
		if err := b.InitiateBenthosStop(ctx); err != nil {
			return err
		}
		// Send event to transition to Stopping.
		return b.sendEvent(ctx, EventStop)
	case OperationalStateStopping:
		// If already stopping, verify if the instance is completely stopped.
		if b.IsBenthosStopped() {
			// Transition from Stopping to Stopped.
			return b.sendEvent(ctx, EventStopDone)
		}
		return nil
	default:
		return fmt.Errorf("invalid current state: %s", currentState)
	}
}
