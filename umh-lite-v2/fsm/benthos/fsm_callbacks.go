package benthos

import (
	"context"
	"log"

	"github.com/looplab/fsm"
)

// RegisterCallbacks registers common callbacks for state transitions
// These callbacks are executed synchronously and should not have any network calls or other operations that could fail

func (instance *BenthosInstance) RegisterCallbacks() {

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

	instance.callbacks["enter_"+LifecycleStateRemoved] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is removed", instance.ID)
	}

	instance.callbacks["enter_"+LifecycleStateCreating] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is creating", instance.ID)
	}

	instance.callbacks["enter_"+LifecycleStateToBeCreated] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is to be created", instance.ID)
	}

	instance.callbacks["enter_"+LifecycleStateRemoving] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is removing", instance.ID)
	}

}
