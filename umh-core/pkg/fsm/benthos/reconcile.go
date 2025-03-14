package benthos

import (
	"context"
	"errors"
	"fmt"
	"time"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
)

// Reconcile examines the BenthosInstance and, in three steps:
//  1. Detect any external changes (e.g., a new configuration or external signals).
//  2. Check if a previous transition failed; if so, verify whether the backoff has elapsed.
//  3. Attempt the required state transition by sending the appropriate event.
//
// This function is intended to be called repeatedly (e.g. in a periodic control loop).
// Over multiple calls, it converges the actual state to the desired state. Transitions
// that fail are retried in subsequent reconcile calls after a backoff period.
func (b *BenthosInstance) Reconcile(ctx context.Context) (err error, reconciled bool) {
	defer func() {
		if err != nil {
			b.baseFSMInstance.GetLogger().Errorf("error reconciling Benthos instance %s: %s", b.baseFSMInstance.GetID(), err)
			b.PrintState()
		}
	}()

	// Measure reconcile time
	start := time.Now()

	// Step 1: Detect external changes.
	if err := b.reconcileExternalChanges(ctx); err != nil {
		// If the service is not running, we don't want to return an error here,
		// because we want to continue reconciling
		if !errors.Is(err, fmt.Errorf("service not exist")) { // Replace with appropriate error when available
			return fmt.Errorf("error reconciling external changes: %w", err), false
		}
		// The service does not exist, which is fine as this happens in the reconcileStateTransition
	}
	externalChangesTime := time.Since(start)
	b.baseFSMInstance.GetLogger().Debugf("Reconcile external changes took %v", externalChangesTime)

	start = time.Now()
	// Step 2: If there's a lastError, see if we've waited enough.
	if b.baseFSMInstance.ShouldSkipReconcileBecauseOfError() {
		return nil, false
	}
	skipErrorTime := time.Since(start)
	b.baseFSMInstance.GetLogger().Debugf("Reconcile skip error took %v", skipErrorTime)

	start = time.Now()
	// Step 3: Attempt to reconcile the state.
	err = b.reconcileStateTransition(ctx)
	if err != nil {
		b.baseFSMInstance.SetError(err)
		b.baseFSMInstance.GetLogger().Errorf("error reconciling state: %s", err)
		return nil, false // We don't want to return an error here, because we want to continue reconciling
	}
	reconcileStateTime := time.Since(start)
	b.baseFSMInstance.GetLogger().Debugf("Reconcile state transition took %v", reconcileStateTime)

	// It went all right, so clear the error
	b.baseFSMInstance.ResetState()

	return nil, true
}

// reconcileExternalChanges checks if the BenthosInstance service status has changed
// externally (e.g., if someone manually stopped or started it, or if it crashed)
func (b *BenthosInstance) reconcileExternalChanges(ctx context.Context) error {
	err := b.updateObservedState(ctx)
	if err != nil {
		return fmt.Errorf("failed to update observed state: %w", err)
	}

	// TODO: Add Benthos-specific external state monitoring
	// For example:
	// - Check Benthos metrics endpoint for data processing stats
	// - Check logs for warnings or errors
	// - Check if Benthos is still properly responding on its API

	return nil
}

// reconcileStateTransition compares the current state with the desired state
// and, if necessary, sends events to drive the FSM from the current to the desired state.
// Any functions that fetch information are disallowed here and must be called in reconcileExternalChanges
// and exist in ObservedState.
// This is to ensure full testability of the FSM.
func (b *BenthosInstance) reconcileStateTransition(ctx context.Context) error {
	currentState := b.baseFSMInstance.GetCurrentFSMState()
	desiredState := b.baseFSMInstance.GetDesiredFSMState()

	// If already in the desired state, nothing to do.
	if currentState == desiredState {
		return nil
	}

	// Handle lifecycle states first - these take precedence over operational states
	if internal_fsm.IsLifecycleState(currentState) {
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
	case internal_fsm.LifecycleStateToBeCreated:
		if err := b.initiateBenthosCreate(ctx); err != nil {
			return err
		}
		return b.baseFSMInstance.SendEvent(ctx, internal_fsm.LifecycleEventCreate)
	case internal_fsm.LifecycleStateCreating:
		// Check if the service is created
		// For now, we'll assume it's created immediately after initiating creation
		return b.baseFSMInstance.SendEvent(ctx, internal_fsm.LifecycleEventCreateDone)
	case internal_fsm.LifecycleStateRemoving:
		if err := b.initiateBenthosRemove(ctx); err != nil {
			return err
		}
		return b.baseFSMInstance.SendEvent(ctx, internal_fsm.LifecycleEventRemoveDone)
	case internal_fsm.LifecycleStateRemoved:
		return fmt.Errorf("instance %s is removed", b.baseFSMInstance.GetID())
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
	// TODO: Add Benthos-specific transition logic through more granular states
	// like ConfigLoading, InitializingComponents, etc.

	// Basic transition logic based on s6 pattern
	if currentState == OperationalStateStopped {
		// Attempt to initiate start
		if err := b.initiateBenthosStart(ctx); err != nil {
			return err
		}
		// Send event to transition from Stopped to Starting
		return b.baseFSMInstance.SendEvent(ctx, EventStart)
	}

	if currentState == OperationalStateStarting {
		// If already in the process of starting, check if the service is running
		if b.IsBenthosRunning() {
			// TODO: Add additional health checks for Benthos
			// For example, check if Benthos is reporting ready on its health endpoint

			// Transition from Starting to Running
			return b.baseFSMInstance.SendEvent(ctx, EventStartDone)
		}
		// Otherwise, wait for the next reconcile cycle
		return nil
	}

	return nil
}

// reconcileTransitionToStopped handles transitions when the desired state is Stopped.
// It deals with moving from Running/Starting to Stopping and then to Stopped.
func (b *BenthosInstance) reconcileTransitionToStopped(ctx context.Context, currentState string) error {
	if currentState == OperationalStateRunning || currentState == OperationalStateStarting {
		// Attempt to initiate a stop
		if err := b.initiateBenthosStop(ctx); err != nil {
			return err
		}
		// Send event to transition to Stopping
		return b.baseFSMInstance.SendEvent(ctx, EventStop)
	}

	if currentState == OperationalStateStopping {
		// If already stopping, verify if the instance is completely stopped
		if b.IsBenthosStopped() {
			// Transition from Stopping to Stopped
			return b.baseFSMInstance.SendEvent(ctx, EventStopDone)
		}
		return nil
	}

	return nil
}

// TODO: Add Benthos-specific reconciliation methods for advanced states
// Examples:
// - reconcileTransitionToProcessing(ctx, currentState) - Handle transitions to processing state
// - reconcileTransitionToProcessingWithWarnings(ctx, currentState) - Handle transitions when warnings appear
// - reconcileTransitionToProcessingWithErrors(ctx, currentState) - Handle transitions when errors appear
