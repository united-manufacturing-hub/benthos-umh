package s6

import (
	"context"
	"log"

	"github.com/looplab/fsm"
)

// RegisterCallbacks registers common callbacks for state transitions
// These callbacks are executed synchronously and should not have any network calls or other operations that could fail
func (instance *S6Instance) RegisterCallbacks() {
	instance.callbacks["enter_"+StateStarting] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] S6 service %s is starting", instance.ID)
	}

	instance.callbacks["enter_"+StateStopping] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] S6 service %s is stopping", instance.ID)
	}

	instance.callbacks["enter_"+StateStopped] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] S6 service %s is stopped", instance.ID)
	}

	instance.callbacks["enter_"+StateRunning] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] S6 service %s is running", instance.ID)
	}

	instance.callbacks["enter_"+StateFailed] = func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] S6 service %s has failed", instance.ID)
	}
}
