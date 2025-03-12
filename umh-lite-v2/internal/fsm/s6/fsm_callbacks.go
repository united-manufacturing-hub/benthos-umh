package s6

import (
	"context"
	"log"

	"github.com/looplab/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/fsm/utils"
)

// registerCallbacks registers common callbacks for state transitions
// These callbacks are executed synchronously and should not have any network calls or other operations that could fail
func (instance *S6Instance) registerCallbacks() {
	instance.callbacks["enter_"+OperationalStateStarting] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is starting", instance.ID)
	}

	instance.callbacks["enter_"+OperationalStateStopping] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is stopping", instance.ID)
	}

	instance.callbacks["enter_"+OperationalStateStopped] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is stopped", instance.ID)
	}

	instance.callbacks["enter_"+OperationalStateRunning] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is running", instance.ID)
	}

	instance.callbacks["enter_"+utils.LifecycleStateRemoved] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is removed", instance.ID)
	}

	instance.callbacks["enter_"+utils.LifecycleStateCreating] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is creating", instance.ID)
	}

	instance.callbacks["enter_"+utils.LifecycleStateToBeCreated] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is to be created", instance.ID)
	}

	instance.callbacks["enter_"+utils.LifecycleStateRemoving] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is removing", instance.ID)
	}

	instance.callbacks["enter_"+OperationalStateUnknown] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is unknown", instance.ID)
	}
}
