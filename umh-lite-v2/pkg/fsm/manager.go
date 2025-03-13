// Interface for all FSM managers

package fsm

import (
	"context"

	config "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/pkg/config"
)

type FSMManager interface {
	Reconcile(ctx context.Context, cfg config.FullConfig) error
}
