package benthos

import (
	"context"

	"github.com/looplab/fsm"
)

// registerCallbacks registers common callbacks for state transitions
// These callbacks are executed synchronously and should not have any network calls or other operations that could fail
func (instance *BenthosInstance) registerCallbacks() {
	// Basic operational state callbacks
	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateStarting, func(ctx context.Context, e *fsm.Event) {
		instance.baseFSMInstance.GetLogger().Infof("Entering starting state for %s", instance.baseFSMInstance.GetID())
	})

	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateStopping, func(ctx context.Context, e *fsm.Event) {
		instance.baseFSMInstance.GetLogger().Infof("Entering stopping state for %s", instance.baseFSMInstance.GetID())
	})

	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateStopped, func(ctx context.Context, e *fsm.Event) {
		instance.baseFSMInstance.GetLogger().Infof("Entering stopped state for %s", instance.baseFSMInstance.GetID())
	})

	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateRunning, func(ctx context.Context, e *fsm.Event) {
		instance.baseFSMInstance.GetLogger().Infof("Entering running state for %s", instance.baseFSMInstance.GetID())
	})

	// TODO: Add callbacks for Benthos-specific states when they are finalized
	// Example callbacks for potential states:

	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateConfigLoading, func(ctx context.Context, e *fsm.Event) {
		instance.baseFSMInstance.GetLogger().Infof("Entering config loading state for %s", instance.baseFSMInstance.GetID())
	})

	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateConfigInvalid, func(ctx context.Context, e *fsm.Event) {
		instance.baseFSMInstance.GetLogger().Infof("Entering config invalid state for %s", instance.baseFSMInstance.GetID())
		// Additional logic for handling invalid configs could be added here
	})

	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateInitializingComponents, func(ctx context.Context, e *fsm.Event) {
		instance.baseFSMInstance.GetLogger().Infof("Entering initializing components state for %s", instance.baseFSMInstance.GetID())
	})

	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateProcessing, func(ctx context.Context, e *fsm.Event) {
		instance.baseFSMInstance.GetLogger().Infof("Entering processing state for %s", instance.baseFSMInstance.GetID())
	})

	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateProcessingWithWarnings, func(ctx context.Context, e *fsm.Event) {
		instance.baseFSMInstance.GetLogger().Warnf("Entering processing with warnings state for %s", instance.baseFSMInstance.GetID())
		// Additional logic for handling warnings could be added here
	})

	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateProcessingWithErrors, func(ctx context.Context, e *fsm.Event) {
		instance.baseFSMInstance.GetLogger().Errorf("Entering processing with errors state for %s", instance.baseFSMInstance.GetID())
		// Additional logic for handling errors could be added here
	})

	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateIdle, func(ctx context.Context, e *fsm.Event) {
		instance.baseFSMInstance.GetLogger().Infof("Entering idle state for %s", instance.baseFSMInstance.GetID())
	})
}
