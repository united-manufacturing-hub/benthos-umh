package benthos

import (
	"context"
	"log"

	"github.com/looplab/fsm"
)

// RegisterCallbacks registers common callbacks for state transitions
// These callbacks are executed synchronously and should not have any network calls or other operations that could fail

func (instance *BenthosInstance) RegisterCallbacks() {

	instance.callbacks["enter_"+StateStarting] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is starting", instance.ID)
	}

	instance.callbacks["enter_"+StateStopping] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is stopping", instance.ID)
	}

	instance.callbacks["enter_"+StateStopped] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is stopped", instance.ID)
	}

	instance.callbacks["enter_"+StateRunning] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is running", instance.ID)
	}

}
