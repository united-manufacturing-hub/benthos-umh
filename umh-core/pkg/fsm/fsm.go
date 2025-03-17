package fsm

import (
	"context"
)

// FSMInstance is the interface that all FSM instances must implement
type FSMInstance interface {
	// GetCurrentFSMState returns the current state of the FSM
	GetCurrentFSMState() string
	// SetDesiredFSMState sets the desired state of the FSM
	SetDesiredFSMState(state string) error
	// GetDesiredFSMState returns the desired state of the FSM
	GetDesiredFSMState() string
	// Remove starts the removal process
	Remove(ctx context.Context) error
	// IsRemoved returns true if the instance has been removed
	IsRemoved() bool
	// Reconcile performs reconciliation of the FSM
	Reconcile(ctx context.Context) (error, bool)
	// PrintState prints the current state of the FSM
	PrintState()
}
