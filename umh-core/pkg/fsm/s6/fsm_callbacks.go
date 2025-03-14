package s6

import (
	"context"
	"log"

	"github.com/looplab/fsm"
)

// registerCallbacks registers common callbacks for state transitions
// These callbacks are executed synchronously and should not have any network calls or other operations that could fail
func (instance *S6Instance) registerCallbacks() {
	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateStarting, func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is starting", instance.baseFSMInstance.GetID())
	})

	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateStopping, func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is stopping", instance.baseFSMInstance.GetID())
	})

	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateStopped, func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is stopped", instance.baseFSMInstance.GetID())
	})

	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateRunning, func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is running", instance.baseFSMInstance.GetID())
	})

	instance.baseFSMInstance.AddCallback("enter_"+OperationalStateUnknown, func(ctx context.Context, e *fsm.Event) {
		log.Printf("[FSM] Benthos instance %s is unknown", instance.baseFSMInstance.GetID())
	})
}
