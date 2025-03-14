package s6

import (
	"context"
	"fmt"
	"log"
	"path/filepath"

	"github.com/looplab/fsm"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/s6"
)

// NewS6Instance creates a new S6Instance with the given ID and service path
func NewS6Instance(
	s6BaseDir string,
	config config.S6FSMConfig) *S6Instance {

	cfg := internal_fsm.BaseFSMInstanceConfig{
		ID:                           config.Name,
		DesiredFSMState:              OperationalStateStopped,
		OperationalStateAfterCreate:  OperationalStateStopped,
		OperationalStateBeforeRemove: OperationalStateStopped,
		OperationalTransitions: []fsm.EventDesc{
			// Operational transitions (only valid when lifecycle state is "created")
			{Name: EventStart, Src: []string{OperationalStateStopped}, Dst: OperationalStateStarting},
			{Name: EventStartDone, Src: []string{OperationalStateStarting}, Dst: OperationalStateRunning},

			// Running/Starting -> Stopping -> Stopped
			{Name: EventStop, Src: []string{OperationalStateRunning, OperationalStateStarting}, Dst: OperationalStateStopping},
			{Name: EventStopDone, Src: []string{OperationalStateStopping}, Dst: OperationalStateStopped},

			// Restart from any state
			{Name: EventRestart, Src: []string{OperationalStateRunning, OperationalStateStopped}, Dst: OperationalStateStarting},
		},
	}

	instance := &S6Instance{
		baseFSMInstance: internal_fsm.NewBaseFSMInstance(cfg),
		servicePath:     filepath.Join(s6BaseDir, config.Name),
		config:          config,
		service:         s6service.NewDefaultService(),
	}

	instance.registerCallbacks()

	return instance
}

// NewS6InstanceWithService creates a new S6Instance with a custom service implementation
// This is useful for testing
func NewS6InstanceWithService(
	s6BaseDir string,
	config config.S6FSMConfig,
	service s6service.Service) *S6Instance {
	instance := NewS6Instance(s6BaseDir, config)
	instance.service = service
	return instance
}

// SetDesiredFSMState safely updates the desired state
// But ensures that the desired state is a valid state and that it is also a reasonable state
// e.g., nobody wants to have an instance in the "starting" state, that is just intermediate
func (s *S6Instance) SetDesiredFSMState(state string) error {
	if state != OperationalStateRunning && state != OperationalStateStopped {
		return fmt.Errorf("invalid desired state: %s. valid states are %s and %s", state, OperationalStateRunning, OperationalStateStopped)
	}

	s.baseFSMInstance.SetDesiredFSMState(state)
	return nil
}

// GetCurrentFSMState returns the current state of the FSM
func (s *S6Instance) GetCurrentFSMState() string {
	return s.baseFSMInstance.GetCurrentFSMState()
}

// GetDesiredFSMState returns the desired state of the FSM
func (s *S6Instance) GetDesiredFSMState() string {
	return s.baseFSMInstance.GetDesiredFSMState()
}

// Remove starts the removal process, it is idempotent and can be called multiple times
// Note: it is only removed once IsRemoved returns true
func (s *S6Instance) Remove(ctx context.Context) error {
	return s.baseFSMInstance.Remove(ctx)
}

func (s *S6Instance) IsRemoved() bool {
	return s.baseFSMInstance.IsRemoved()
}

func (s *S6Instance) PrintState() {
	log.Printf("[S6Instance] Current state: %s", s.baseFSMInstance.GetCurrentFSMState())
	log.Printf("[S6Instance] Desired state: %s", s.baseFSMInstance.GetDesiredFSMState())
	log.Printf("[S6Instance] Observed state: %+v", s.ObservedState)
}
