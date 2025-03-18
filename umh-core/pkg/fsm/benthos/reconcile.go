package benthos

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
)

// Reconcile examines the BenthosInstance and, in three steps:
//  1. Detect any external changes (e.g., a new configuration or external signals).
//  2. Check if a previous transition failed; if so, verify whether the backoff has elapsed.
//  3. Attempt the required state transition by sending the appropriate event.
//
// This function is intended to be called repeatedly (e.g. in a periodic control loop).
// Over multiple calls, it converges the actual state to the desired state. Transitions
// that fail are retried in subsequent reconcile calls after a backoff period.
func (b *BenthosInstance) Reconcile(ctx context.Context, tick uint64) (err error, reconciled bool) {
	instanceName := b.baseFSMInstance.GetID()
	defer func() {
		// Log errors for debugging
		if err != nil {
			b.baseFSMInstance.GetLogger().Errorf("error reconciling Benthos instance %s: %s", instanceName, err)
			b.PrintState()
		}

		// Recover from panics
		if r := recover(); r != nil {
			b.baseFSMInstance.GetLogger().Errorf("Panic during reconcile for %s: %v", instanceName, r)
			debug.PrintStack()
		}
	}()

	// Skip reconciliation if in error backoff
	if b.baseFSMInstance.ShouldSkipReconcileBecauseOfError(tick) {
		err := b.baseFSMInstance.GetBackoffError(tick)
		b.baseFSMInstance.GetLogger().Debugf("Skipping reconcile for Benthos pipeline %s: %s", instanceName, err)
		return nil, false
	}

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
	if b.baseFSMInstance.ShouldSkipReconcileBecauseOfError(tick) {
		return nil, false
	}
	skipErrorTime := time.Since(start)
	b.baseFSMInstance.GetLogger().Debugf("Reconcile skip error took %v", skipErrorTime)

	start = time.Now()
	// Step 3: Attempt to reconcile the state.
	err = b.reconcileStateTransition(ctx)
	if err != nil {
		b.baseFSMInstance.SetError(err, tick)
		b.baseFSMInstance.GetLogger().Errorf("error reconciling state: %s", err)
		return nil, false // We don't want to return an error here, because we want to continue reconciling
	}
	reconcileStateTime := time.Since(start)
	b.baseFSMInstance.GetLogger().Debugf("Reconcile state transition took %v", reconcileStateTime)

	// Reconcile the s6Manager, on purpose we pass the full config where the rest of the
	// BenthosInstance is embedded, but the rest is removed to prevent leaking abstractions
	err, reconciled = b.s6Manager.Reconcile(ctx, config.FullConfig{Services: b.s6ServiceConfigs}, tick)
	if err != nil {
		b.baseFSMInstance.SetError(err, tick)
		b.baseFSMInstance.GetLogger().Errorf("error reconciling s6Manager: %s", err)
		return nil, false // We don't want to return an error here, because we want to continue reconciling
	}
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
	case OperationalStateActive:
		return b.reconcileTransitionToActive(ctx, currentState)
	case OperationalStateStopped:
		return b.reconcileTransitionToStopped(ctx, currentState)
	default:
		return fmt.Errorf("invalid desired state: %s", desiredState)
	}
}

// reconcileTransitionToActive handles transitions when the desired state is Active.
// It deals with moving from various states to the Active state.
func (b *BenthosInstance) reconcileTransitionToActive(ctx context.Context, currentState string) error {
	// If we're stopped, we need to start first
	if currentState == OperationalStateStopped {
		// Attempt to initiate start
		if err := b.initiateBenthosStart(ctx); err != nil {
			return err
		}
		// Send event to transition from Stopped to Starting
		return b.baseFSMInstance.SendEvent(ctx, EventStart)
	}

	// Handle starting phase states
	if IsStartingState(currentState) {
		// If in the starting phase, progress through the starting states
		switch currentState {
		case OperationalStateStarting:
			// Check if config is loaded
			// TODO: Implement actual config loading check
			return b.baseFSMInstance.SendEvent(ctx, EventConfigLoaded)
		case OperationalStateStartingConfigLoading:
			// Check whether the config is loaded by checkign if it crashes immediately
			// TODO: Implement actual healthcheck verification
			return b.baseFSMInstance.SendEvent(ctx, EventConfigLoaded)
		case OperationalStateStartingWaitingForHealthchecks:
			// TODO: Implement actual healthcheck verification
			return b.baseFSMInstance.SendEvent(ctx, EventHealthchecksPassed)
		}
	}

	// If we're in Idle, we need to detect data to move to Active
	if currentState == OperationalStateIdle {
		// Check if data is being processed
		// TODO: Implement actual data processing detection
		// For now, we'll simulate data being received
		return b.baseFSMInstance.SendEvent(ctx, EventDataReceived)
	}

	// If we're in Degraded, we need to recover to move to Active
	if currentState == OperationalStateDegraded {
		// Check if the service has recovered
		// TODO: Implement actual recovery detection
		// For now, we'll simulate recovery
		return b.baseFSMInstance.SendEvent(ctx, EventRecovered)
	}

	return nil
}

// reconcileTransitionToStopped handles transitions when the desired state is Stopped.
// It deals with moving from any operational state to Stopping and then to Stopped.
func (b *BenthosInstance) reconcileTransitionToStopped(ctx context.Context, currentState string) error {
	// If we're in any operational state except Stopped or Stopping, initiate stop
	if currentState != OperationalStateStopped && currentState != OperationalStateStopping {
		// Attempt to initiate a stop
		if err := b.initiateBenthosStop(ctx); err != nil {
			return err
		}
		// Send event to transition to Stopping
		return b.baseFSMInstance.SendEvent(ctx, EventStop)
	}

	// If we're in Stopping, check if the service has stopped
	if currentState == OperationalStateStopping {
		// TODO: Implement actual stopping verification
		return b.baseFSMInstance.SendEvent(ctx, EventStopDone)
	}
	return nil
}

// TODO: Add Benthos-specific reconciliation methods for advanced states
// Examples:
// - reconcileTransitionToProcessing(ctx, currentState) - Handle transitions to processing state
// - reconcileTransitionToProcessingWithWarnings(ctx, currentState) - Handle transitions when warnings appear
// - reconcileTransitionToProcessingWithErrors(ctx, currentState) - Handle transitions when errors appear
