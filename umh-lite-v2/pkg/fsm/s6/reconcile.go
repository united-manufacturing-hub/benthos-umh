package s6

import (
	"context"
	"errors"
	"fmt"
	"log"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/fsm"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/pkg/service/s6"
)

// Reconcile examines the S6Instance and, in three steps:
//  1. Detect any external changes (e.g., a new configuration or external signals).
//  2. Check if a previous transition failed; if so, verify whether the backoff has elapsed.
//  3. Attempt the required state transition by sending the appropriate event.
//
// This function is intended to be called repeatedly (e.g. in a periodic control loop).
// Over multiple calls, it converges the actual state to the desired state. Transitions
// that fail are retried in subsequent reconcile calls after a backoff period.
func (s *S6Instance) Reconcile(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			log.Printf("[S6Instance] error reconciling: %s", err)
			s.PrintState()
		}
	}()

	// Step 1: Detect external changes.
	if err := s.reconcileExternalChanges(ctx); err != nil {
		// If the service is not running, we don't want to return an error here, because we want to continue reconciling
		if !errors.Is(err, s6service.ErrServiceNotExist) {
			log.Printf("[S6Instance] error reconciling external changes: %s", err)
			return err
		}

		// The service does not exist, which is fine as this happens in the reconcileStateTransition
	}

	// Step 2: If there's a lastError, see if we've waited enough.
	if s.baseFSMInstance.ShouldSkipReconcileBecauseOfError() {
		return nil
	}

	// Step 3: Attempt to reconcile the state.
	err = s.reconcileStateTransition(ctx)
	if err != nil {
		s.baseFSMInstance.SetError(err)
		log.Printf("[S6Instance] error reconciling state: %s", err)
		return nil // We don't want to return an error here, because we want to continue reconciling
	}

	// It went all right, so clear the error
	s.baseFSMInstance.ResetState()

	return nil
}

// reconcileExternalChanges checks if the S6Instance service status has changed
// externally (e.g., if someone manually stopped or started it, or if it crashed)
func (s *S6Instance) reconcileExternalChanges(ctx context.Context) error {
	err := s.updateObservedState(ctx)
	if err != nil {
		return err
	}
	return nil
}

// reconcileStateTransition compares the current state with the desired state
// and, if necessary, sends events to drive the FSM from the current to the desired state.
// Any functions that fetch information are disallowed here and must be called in reconcileExternalChanges
// and exist in ExternalState.
// This is to ensure full testability of the FSM.
func (s *S6Instance) reconcileStateTransition(ctx context.Context) error {
	currentState := s.baseFSMInstance.GetCurrentFSMState()
	desiredState := s.baseFSMInstance.GetDesiredFSMState()

	// If already in the desired state, nothing to do.
	if currentState == desiredState {
		return nil
	}

	// Handle lifecycle states first - these take precedence over operational states
	if internal_fsm.IsLifecycleState(currentState) {
		return s.reconcileLifecycleStates(ctx, currentState)
	}

	// Handle operational states
	if IsOperationalState(currentState) {
		return s.reconcileOperationalStates(ctx, currentState, desiredState)
	}

	return fmt.Errorf("invalid state: %s", currentState)
}

// reconcileLifecycleStates handles states related to instance lifecycle (creating/removing)
func (b *S6Instance) reconcileLifecycleStates(ctx context.Context, currentState string) error {
	// Independent what the desired state is, we always need to reconcile the lifecycle states first
	switch currentState {
	case internal_fsm.LifecycleStateToBeCreated:
		if err := b.initiateS6Create(ctx); err != nil {
			return err
		}
		return b.baseFSMInstance.SendEvent(ctx, internal_fsm.LifecycleEventCreate)
	case internal_fsm.LifecycleStateCreating:
		// TODO: check if the service is created
		return b.baseFSMInstance.SendEvent(ctx, internal_fsm.LifecycleEventCreateDone)
	case internal_fsm.LifecycleStateRemoving:
		if err := b.initiateS6Remove(ctx); err != nil {
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
func (b *S6Instance) reconcileOperationalStates(ctx context.Context, currentState string, desiredState string) error {
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
// It deals with moving from Stopped/Failed to Starting and then to Running.
func (s *S6Instance) reconcileTransitionToRunning(ctx context.Context, currentState string) error {
	if currentState == OperationalStateStopped {
		// Attempt to initiate start
		if err := s.initiateS6Start(ctx); err != nil {
			return err
		}
		// Send event to transition from Stopped/Failed to Starting
		return s.baseFSMInstance.SendEvent(ctx, EventStart)
	}

	if currentState == OperationalStateStarting {
		// If already in the process of starting, check if the service is healthy
		if s.IsS6Running() {
			// Transition from Starting to Running
			return s.baseFSMInstance.SendEvent(ctx, EventStartDone)
		}
		// Otherwise, wait for the next reconcile cycle
		return nil
	}

	return nil
}

// reconcileTransitionToStopped handles transitions when the desired state is Stopped.
// It deals with moving from Running/Starting/Failed to Stopping and then to Stopped.
func (s *S6Instance) reconcileTransitionToStopped(ctx context.Context, currentState string) error {
	if currentState == OperationalStateRunning || currentState == OperationalStateStarting {
		// Attempt to initiate a stop
		if err := s.initiateS6Stop(ctx); err != nil {
			return err
		}
		// Send event to transition to Stopping
		return s.baseFSMInstance.SendEvent(ctx, EventStop)
	}

	if currentState == OperationalStateStopping {
		// If already stopping, verify if the instance is completely stopped
		if s.IsS6Stopped() {
			// Transition from Stopping to Stopped
			return s.baseFSMInstance.SendEvent(ctx, EventStopDone)
		}
		return nil
	}

	return nil
}
