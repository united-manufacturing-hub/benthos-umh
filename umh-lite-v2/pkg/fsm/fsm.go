package fsm

import "context"

// FSMInstance represents a finite state machine instance
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
}
