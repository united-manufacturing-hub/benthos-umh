package s6

import (
	"fmt"
	"path/filepath"

	"github.com/looplab/fsm"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/fsm"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/pkg/service/s6"
)

// NewS6Instance creates a new S6Instance with the given ID and service path
func NewS6Instance(
	id string,
	servicePath string,
	config s6service.ServiceConfig) *S6Instance {

	cfg := internal_fsm.BaseFSMInstanceConfig{
		ID:                            id,
		DesiredFSMState:               OperationalStateStopped,
		OperationalStateAfterCreate:   OperationalStateStopped,
		OperationalStatesBeforeRemove: []string{OperationalStateStopped},
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
		servicePath:     servicePath,
		config:          config,
		service:         s6service.NewDefaultService(),
		ObservedState:   S6ObservedState{Status: S6ServiceUnknown, ServiceInfo: s6service.ServiceInfo{Status: s6service.ServiceUnknown}},
	}

	instance.registerCallbacks()

	return instance
}

// NewServiceInBaseDir creates a new S6Instance in the given base directory
func NewServiceInBaseDir(
	id string,
	baseDir string,
	config s6service.ServiceConfig) *S6Instance {
	// Construct the full service path
	servicePath := filepath.Join(baseDir, id)
	return NewS6Instance(id, servicePath, config)
}

// NewS6InstanceWithService creates a new S6Instance with a custom service implementation
func NewS6InstanceWithService(
	id string,
	servicePath string,
	service s6service.Service,
	config s6service.ServiceConfig) *S6Instance {
	instance := NewS6Instance(id, servicePath, config)
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
