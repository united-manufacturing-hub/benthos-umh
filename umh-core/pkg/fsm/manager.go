// Interface for all FSM managers

package fsm

import (
	"context"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
)

// FSMManager is a generic interface for managing FSM instances
type FSMManager[C any] interface {
	// Reconcile reconciles the current state with the desired state
	Reconcile(ctx context.Context, config config.FullConfig) (error, bool)
	// Additional common methods
}
