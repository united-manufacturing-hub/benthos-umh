package benthos

import (
	"context"
	"errors"
	"fmt"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/metrics"
	benthos_service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/benthos"
)

// Reconcile examines the BenthosInstance and, in three steps:
//  1. Check if a previous transition failed or if fetching external state failed; if so, verify whether the backoff has elapsed.
//  2. Detect any external changes (e.g., a new configuration or external signals).
//  3. Attempt the required state transition by sending the appropriate event.
//
// This function is intended to be called repeatedly (e.g. in a periodic control loop).
// Over multiple calls, it converges the actual state to the desired state. Transitions
// that fail are retried in subsequent reconcile calls after a backoff period.
func (b *BenthosInstance) Reconcile(ctx context.Context, tick uint64) (err error, reconciled bool) {
	benthosInstanceName := b.baseFSMInstance.GetID()
	defer func() {
		if err != nil {
			b.baseFSMInstance.GetLogger().Errorf("error reconciling Benthos instance %s: %s", benthosInstanceName, err)
			b.PrintState()
			// Add metrics for error
			metrics.IncErrorCount(metrics.ComponentBenthosInstance, benthosInstanceName)
		}
	}()

	// Check if context is already cancelled
	if ctx.Err() != nil {
		return ctx.Err(), false
	}

	// Step 1: If there's a lastError, see if we've waited enough.
	if b.baseFSMInstance.ShouldSkipReconcileBecauseOfError(tick) {
		err := b.baseFSMInstance.GetBackoffError(tick)
		b.baseFSMInstance.GetLogger().Debugf("Skipping reconcile for Benthos pipeline %s: %s", benthosInstanceName, err)
		return nil, false
	}

	// Step 2: Detect external changes.
	if err := b.reconcileExternalChanges(ctx, tick); err != nil {
		// If the service is not running, we don't want to return an error here, because we want to continue reconciling
		if !errors.Is(err, benthos_service.ErrServiceNotExist) {
			b.baseFSMInstance.SetError(err, tick)
			b.baseFSMInstance.GetLogger().Errorf("error reconciling external changes: %s", err)
			return nil, false // We don't want to return an error here, because we want to continue reconciling
		}

		err = nil // The service does not exist, which is fine as this happens in the reconcileStateTransition
	}

	// Step 3: Attempt to reconcile the state.
	err, reconciled = b.reconcileStateTransition(ctx)
	if err != nil {
		// If the instance is removed, we don't want to return an error here, because we want to continue reconciling
		if errors.Is(err, fsm.ErrInstanceRemoved) {
			return nil, false
		}

		b.baseFSMInstance.SetError(err, tick)
		b.baseFSMInstance.GetLogger().Errorf("error reconciling state: %s", err)
		return nil, false // We don't want to return an error here, because we want to continue reconciling
	}

	// Reconcile the s6Manager
	s6Err, s6Reconciled := b.service.ReconcileManager(ctx, tick)
	if s6Err != nil {
		b.baseFSMInstance.SetError(s6Err, tick)
		b.baseFSMInstance.GetLogger().Errorf("error reconciling s6Manager: %s", s6Err)
		return nil, false
	}

	// If either Benthos state or S6 state was reconciled, we return reconciled so that nothing happens anymore in this tick
	// nothing should happen as we might have already taken up some significant time of the avaialble time per tick, so better
	// to be on the safe side and let the rest handle in another tick
	reconciled = reconciled || s6Reconciled

	// It went all right, so clear the error
	b.baseFSMInstance.ResetState()

	return nil, reconciled
}

// reconcileExternalChanges checks if the BenthosInstance service status has changed
// externally (e.g., if someone manually stopped or started it, or if it crashed)
func (b *BenthosInstance) reconcileExternalChanges(ctx context.Context, tick uint64) error {
	err := b.updateObservedState(ctx, tick)
	if err != nil {
		return fmt.Errorf("failed to update observed state: %w", err)
	}
	return nil
}

// reconcileStateTransition compares the current state with the desired state
// and, if necessary, sends events to drive the FSM from the current to the desired state.
// Any functions that fetch information are disallowed here and must be called in reconcileExternalChanges
// and exist in ObservedState.
// This is to ensure full testability of the FSM.
func (b *BenthosInstance) reconcileStateTransition(ctx context.Context) (err error, reconciled bool) {
	currentState := b.baseFSMInstance.GetCurrentFSMState()
	desiredState := b.baseFSMInstance.GetDesiredFSMState()

	// If already in the desired state, nothing to do.
	if currentState == desiredState {
		return nil, false
	}

	// Handle lifecycle states first - these take precedence over operational states
	if internal_fsm.IsLifecycleState(currentState) {
		err, reconciled := b.reconcileLifecycleStates(ctx, currentState)
		if err != nil {
			return err, false
		}
		return nil, reconciled
	}

	// Handle operational states
	if IsOperationalState(currentState) {
		err, reconciled := b.reconcileOperationalStates(ctx, currentState, desiredState)
		if err != nil {
			return err, false
		}
		return nil, reconciled
	}

	return fmt.Errorf("invalid state: %s", currentState), false
}

// reconcileLifecycleStates handles states related to instance lifecycle (creating/removing)
func (b *BenthosInstance) reconcileLifecycleStates(ctx context.Context, currentState string) (err error, reconciled bool) {
	// Independent what the desired state is, we always need to reconcile the lifecycle states first
	switch currentState {
	case internal_fsm.LifecycleStateToBeCreated:
		if err := b.initiateBenthosCreate(ctx); err != nil {
			return err, false
		}
		return b.baseFSMInstance.SendEvent(ctx, internal_fsm.LifecycleEventCreate), true
	case internal_fsm.LifecycleStateCreating:
		// Check if the service is created
		// For now, we'll assume it's created immediately after initiating creation
		return b.baseFSMInstance.SendEvent(ctx, internal_fsm.LifecycleEventCreateDone), true
	case internal_fsm.LifecycleStateRemoving:
		if err := b.initiateBenthosRemove(ctx); err != nil {
			return err, false
		}
		return b.baseFSMInstance.SendEvent(ctx, internal_fsm.LifecycleEventRemoveDone), true
	case internal_fsm.LifecycleStateRemoved:
		return fsm.ErrInstanceRemoved, true
	default:
		// If we are not in a lifecycle state, just continue
		return nil, false
	}
}

// reconcileOperationalStates handles states related to instance operations (starting/stopping)
func (b *BenthosInstance) reconcileOperationalStates(ctx context.Context, currentState string, desiredState string) (err error, reconciled bool) {
	switch desiredState {
	case OperationalStateActive:
		return b.reconcileTransitionToActive(ctx, currentState)
	case OperationalStateStopped:
		return b.reconcileTransitionToStopped(ctx, currentState)
	default:
		return fmt.Errorf("invalid desired state: %s", desiredState), false
	}
}

// reconcileTransitionToActive handles transitions when the desired state is Active.
// It deals with moving from various states to the Active state.
func (b *BenthosInstance) reconcileTransitionToActive(ctx context.Context, currentState string) (err error, reconciled bool) {
	// If we're stopped, we need to start first
	if currentState == OperationalStateStopped {
		// Attempt to initiate start
		if err := b.initiateBenthosStart(ctx); err != nil {
			return err, false
		}
		// Send event to transition from Stopped to Starting
		return b.baseFSMInstance.SendEvent(ctx, EventStart), true
	}

	// Handle starting phase states
	if IsStartingState(currentState) {
		// If in the starting phase, progress through the starting states
		switch currentState {
		case OperationalStateStarting:
			// First we need to ensure the S6 service is started

			if !b.IsBenthosS6Running() {
				return nil, false
			}

			return b.baseFSMInstance.SendEvent(ctx, EventS6Started), true
		case OperationalStateStartingConfigLoading:
			// Check if config has been loaded

			// If the S6 is not running, go back to starting
			if !b.IsBenthosS6Running() {
				return b.baseFSMInstance.SendEvent(ctx, EventStartFailed), true
			}

			// Now check whether benthos has loaded the config
			if !b.IsBenthosConfigLoaded() {
				return nil, false
			}

			return b.baseFSMInstance.SendEvent(ctx, EventConfigLoaded), true
		case OperationalStateStartingWaitingForHealthchecks:
			// If the S6 is not running, go back to starting
			if !b.IsBenthosS6Running() || !b.IsBenthosConfigLoaded() {
				return b.baseFSMInstance.SendEvent(ctx, EventStartFailed), true
			}

			// Check if healthchecks have passed
			if !b.IsBenthosHealthchecksPassed() {
				return nil, false
			}

			return b.baseFSMInstance.SendEvent(ctx, EventHealthchecksPassed), true
		case OperationalStateStartingWaitingForServiceToRemainRunning:
			// If the S6 is not running, go back to starting
			if !b.IsBenthosS6Running() || !b.IsBenthosConfigLoaded() || !b.IsBenthosHealthchecksPassed() {
				return b.baseFSMInstance.SendEvent(ctx, EventStartFailed), true
			}

			// Check if service has been running stably for some time
			if !b.IsBenthosRunningForSomeTime() {
				return nil, false
			}

			return b.baseFSMInstance.SendEvent(ctx, EventStartDone), true
		}
	}

	// Now do all the checks to check whether it is degraded
	if b.IsBenthosDegraded() {
		return b.baseFSMInstance.SendEvent(ctx, EventDegraded), true
	}

	// If we're in Idle, we need to detect data to move to Active
	if currentState == OperationalStateIdle {
		// Check if data is being processed
		// This would be based on metrics data
		hasActivity := b.IsBenthosWithProcessingActivity()
		if hasActivity {
			return b.baseFSMInstance.SendEvent(ctx, EventDataReceived), true
		}
		return nil, false
	}

	// If we're in Degraded, we need to recover to move to Active or Idle
	if currentState == OperationalStateDegraded {
		// Check if the service has recovered
		if !b.IsBenthosDegraded() {
			return b.baseFSMInstance.SendEvent(ctx, EventRecovered), true
		}

		return nil, false
	}

	return nil, false
}

// reconcileTransitionToStopped handles transitions when the desired state is Stopped.
// It deals with moving from any operational state to Stopping and then to Stopped.
func (b *BenthosInstance) reconcileTransitionToStopped(ctx context.Context, currentState string) (err error, reconciled bool) {
	// If we're in any operational state except Stopped or Stopping, initiate stop
	if currentState != OperationalStateStopped && currentState != OperationalStateStopping {
		// Attempt to initiate a stop
		if err := b.initiateBenthosStop(ctx); err != nil {
			return err, false
		}
		// Send event to transition to Stopping
		return b.baseFSMInstance.SendEvent(ctx, EventStop), true
	}

	// If already stopping, verify if the instance is completely stopped
	if b.IsBenthosS6Stopped() {
		// Transition from Stopping to Stopped
		return b.baseFSMInstance.SendEvent(ctx, EventStopDone), true
	}

	return nil, false
}
