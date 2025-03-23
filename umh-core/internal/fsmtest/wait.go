package fsmtest

import (
	"context"
	"fmt"
	"time"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm"
)

// InstanceReconciler is an interface for any FSM instance that can be reconciled
type InstanceReconciler interface {
	Reconcile(ctx context.Context, tick uint64) (error, bool)
	GetCurrentFSMState() string
	GetDesiredFSMState() string
}

// ManagerReconciler is an interface for any FSM manager that can be reconciled
type ManagerReconciler interface {
	Reconcile(ctx context.Context, config config.FullConfig, tick uint64) (error, bool)
	GetInstance(id string) (fsm.FSMInstance, bool)
}

// WaitForInstanceState repeatedly calls Reconcile on an instance until it reaches the desired state or times out
func WaitForInstanceState(ctx context.Context, instance InstanceReconciler, desiredState string, maxAttempts int, startTick uint64) (uint64, error) {
	tick := startTick

	for i := 0; i < maxAttempts; i++ {
		err, _ := instance.Reconcile(ctx, tick)
		if err != nil {
			return tick, fmt.Errorf("error during reconcile: %w", err)
		}
		tick++

		currentState := instance.GetCurrentFSMState()
		if currentState == desiredState {
			return tick, nil
		}
	}

	return tick, fmt.Errorf("failed to reach state %s after %d attempts, current state: %s",
		desiredState, maxAttempts, instance.GetCurrentFSMState())
}

// WaitForManagerInstanceState repeatedly calls Reconcile on a manager until the specified instance
// reaches the desired state or times out
func WaitForManagerInstanceState(ctx context.Context, manager ManagerReconciler, config config.FullConfig,
	instanceID string, desiredState string, maxAttempts int, startTick uint64) (uint64, error) {

	tick := startTick

	for i := 0; i < maxAttempts; i++ {
		err, _ := manager.Reconcile(ctx, config, tick)
		if err != nil {
			return tick, fmt.Errorf("error during manager reconcile: %w", err)
		}
		tick++

		instance, exists := manager.GetInstance(instanceID)
		if !exists {
			// If the desired state is LifecycleStateRemoved, and the instance no longer exists,
			// that's success!
			if desiredState == internal_fsm.LifecycleStateRemoved {
				return tick, nil
			}
			return tick, fmt.Errorf("instance %s not found in manager", instanceID)
		}

		currentState := instance.GetCurrentFSMState()
		if currentState == desiredState {
			return tick, nil
		}
	}

	instance, exists := manager.GetInstance(instanceID)
	if !exists {
		return tick, fmt.Errorf("instance %s not found in manager after %d attempts", instanceID, maxAttempts)
	}

	return tick, fmt.Errorf("instance %s failed to reach state %s after %d attempts, current state: %s",
		instanceID, desiredState, maxAttempts, instance.GetCurrentFSMState())
}

// WithTimeout adds a timeout to the context for test operations
func WithTimeout(parent context.Context, duration time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, duration)
}

// Ticker is a function that takes an action and returns an error if any.
// It's typically used to trigger a reconciliation or other periodic action during tests.
type Ticker func() error

// WaitForFSMState waits for an FSM instance to reach a desired state.
// It uses the provided ticker function to trigger state transitions.
func WaitForFSMState(ctx context.Context, instance fsm.FSMInstance, desiredState string, ticker Ticker) error {
	if ticker == nil {
		ticker = func() error { return nil }
	}

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for instance to reach state %s, current state: %s",
				desiredState, instance.GetCurrentFSMState())
		case <-time.After(100 * time.Millisecond):
			if err := ticker(); err != nil {
				return err
			}

			if instance.GetCurrentFSMState() == desiredState {
				return nil
			}
		}
	}
}

// WaitForInstanceRemoval waits for an instance to be removed from a manager.
// It uses the provided ticker function to trigger state transitions.
func WaitForInstanceRemoval(ctx context.Context, manager ManagerReconciler, instanceID string, ticker Ticker) error {
	if ticker == nil {
		ticker = func() error { return nil }
	}

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for instance %s to be removed", instanceID)
		case <-time.After(100 * time.Millisecond):
			if err := ticker(); err != nil {
				return err
			}

			if _, exists := manager.GetInstance(instanceID); !exists {
				return nil
			}
		}
	}
}
