package benthos

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/looplab/fsm"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/logger"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/s6"
)

// NewBenthosInstance creates a new BenthosInstance with the given ID and service path
func NewBenthosInstance(
	s6BaseDir string,
	config config.S6FSMConfig) *BenthosInstance {

	// TODO: Replace this with proper BenthosConfig when available
	// For now, reusing S6FSMConfig for skeleton implementation

	cfg := internal_fsm.BaseFSMInstanceConfig{
		ID:                           config.Name,
		DesiredFSMState:              OperationalStateStopped,
		OperationalStateAfterCreate:  OperationalStateStopped,
		OperationalStateBeforeRemove: OperationalStateStopped,
		OperationalTransitions: []fsm.EventDesc{
			// Basic operational transitions
			{Name: EventStart, Src: []string{OperationalStateStopped}, Dst: OperationalStateStarting},
			{Name: EventStartDone, Src: []string{OperationalStateStarting}, Dst: OperationalStateRunning},
			{Name: EventStop, Src: []string{OperationalStateRunning, OperationalStateStarting}, Dst: OperationalStateStopping},
			{Name: EventStopDone, Src: []string{OperationalStateStopping}, Dst: OperationalStateStopped},
			{Name: EventRestart, Src: []string{OperationalStateRunning, OperationalStateStopped}, Dst: OperationalStateStarting},

			// TODO: Add Benthos-specific transitions for advanced states
			// These will be added as the state machine design is finalized
		},
	}

	instance := &BenthosInstance{
		baseFSMInstance: internal_fsm.NewBaseFSMInstance(cfg, logger.For(config.Name)),
		servicePath:     filepath.Join(s6BaseDir, config.Name),
		config:          config,
		s6Service:       s6service.NewDefaultService(),
		ObservedState: BenthosObservedState{
			MetricsData: make(map[string]interface{}),
			ConfigValid: true, // Assume valid until proven otherwise
		},
	}

	instance.registerCallbacks()

	return instance
}

// NewBenthosInstanceWithService creates a new BenthosInstance with a custom service implementation
// This is useful for testing
func NewBenthosInstanceWithService(
	s6BaseDir string,
	config config.S6FSMConfig,
	service s6service.Service) *BenthosInstance {
	instance := NewBenthosInstance(s6BaseDir, config)
	instance.s6Service = service
	return instance
}

// SetDesiredFSMState safely updates the desired state
// But ensures that the desired state is a valid state and that it is also a reasonable state
// e.g., nobody wants to have an instance in the "starting" state, that is just intermediate
func (b *BenthosInstance) SetDesiredFSMState(state string) error {
	if state != OperationalStateRunning && state != OperationalStateStopped {
		return fmt.Errorf("invalid desired state: %s. valid states are %s and %s", state, OperationalStateRunning, OperationalStateStopped)
	}

	b.baseFSMInstance.SetDesiredFSMState(state)
	return nil
}

// GetCurrentFSMState returns the current state of the FSM
func (b *BenthosInstance) GetCurrentFSMState() string {
	return b.baseFSMInstance.GetCurrentFSMState()
}

// GetDesiredFSMState returns the desired state of the FSM
func (b *BenthosInstance) GetDesiredFSMState() string {
	return b.baseFSMInstance.GetDesiredFSMState()
}

// Remove starts the removal process, it is idempotent and can be called multiple times
// Note: it is only removed once IsRemoved returns true
func (b *BenthosInstance) Remove(ctx context.Context) error {
	return b.baseFSMInstance.Remove(ctx)
}

// IsRemoved returns true if the instance has been removed
func (b *BenthosInstance) IsRemoved() bool {
	return b.baseFSMInstance.IsRemoved()
}

// PrintState prints the current state of the FSM for debugging
func (b *BenthosInstance) PrintState() {
	b.baseFSMInstance.GetLogger().Debugf("Current state: %s", b.baseFSMInstance.GetCurrentFSMState())
	b.baseFSMInstance.GetLogger().Debugf("Desired state: %s", b.baseFSMInstance.GetDesiredFSMState())
	b.baseFSMInstance.GetLogger().Debugf("Observed state: %+v", b.ObservedState)
}

// TODO: Add Benthos-specific health check methods
// Examples:
// - IsProcessingData() - Checks if Benthos is actively processing data
// - HasWarnings() - Checks if Benthos is reporting warnings
// - HasErrors() - Checks if Benthos is reporting errors
