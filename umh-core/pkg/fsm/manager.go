// Interface for all FSM managers

package fsm

import (
	"context"
)

type FSMManager interface {
	Reconcile(ctx context.Context, cfg interface{}) (error, bool)
}
