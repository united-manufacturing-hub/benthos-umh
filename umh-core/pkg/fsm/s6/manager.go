package s6

import (
	"context"
	"fmt"
	"time"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	public_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/logger"
	"go.uber.org/zap"
)

// S6Manager implements FSM management for S6 services.
type S6Manager struct {
	public_fsm.FSMManager
	Instances map[string]*S6Instance
	logger    *zap.SugaredLogger
}

// NewS6Manager creates a new S6Manager
// The name is used to identify the manager in logs, as other components that leverage s6 will sue their own instance of this manager
func NewS6Manager(name string) *S6Manager {
	return &S6Manager{
		Instances: make(map[string]*S6Instance),
		logger:    logger.For(logger.ComponentS6Manager + name),
	}
}

const (
	baseS6Dir = "/run/service"
)

// Reconcile reconciles desired & observed states for S6 services.
// It returns a boolean indicating if the manager was reconciled.
// The desiredState is a list of S6FSMConfig, which contains the desired state for each instance.
func (m *S6Manager) Reconcile(ctx context.Context, desiredState []config.S6FSMConfig) (error, bool) {

	// Step 1: Detect external changes
	observedState := m.Instances

	// Step 2: Create or update instances
	for _, instance := range desiredState {

		// If the instance does not exist, create it and set it to the desired state
		if _, ok := observedState[instance.Name]; !ok {
			observedState[instance.Name] = NewS6Instance(baseS6Dir, instance)
			observedState[instance.Name].SetDesiredFSMState(instance.DesiredState)
			m.logger.Infof("Created instance %s", instance.Name)
			return nil, true
		}

		// If the instance exists, but the config is different, update it
		if !observedState[instance.Name].config.S6ServiceConfig.Equal(instance.S6ServiceConfig) {
			observedState[instance.Name].config = instance
			m.logger.Infof("Updated config of instance %s", instance.Name)
			return nil, true
		}

		// If the instance exists, but the desired state is different, update it
		if observedState[instance.Name].GetDesiredFSMState() != instance.DesiredState {
			m.logger.Infof("Updated desired state of instance %s from %s to %s", instance.Name, observedState[instance.Name].GetDesiredFSMState(), instance.DesiredState)
			observedState[instance.Name].SetDesiredFSMState(instance.DesiredState)
			return nil, true
		}
	}

	// Step 3: Clean up any instances that are not in desiredState, or are in the removed state
	// Before deletion, they need to be gracefully stopped and we need to wait until they are in the state removed
	for instanceName := range observedState {

		// If the instance is not in desiredState, start its removal process
		found := false
		for _, desired := range desiredState {
			if desired.Name == instanceName {
				found = true
				break
			}
		}

		switch observedState[instanceName].GetCurrentFSMState() {
		case internal_fsm.LifecycleStateRemoving:
			m.logger.Debugf("instance %s is already in removing state, waiting until it is removed", instanceName)
			continue
		case internal_fsm.LifecycleStateRemoved:
			m.logger.Debugf("instance %s is in removed state, deleting it from the manager", instanceName)
			delete(observedState, instanceName)
			continue
		default:
			// If the instance is in desiredState, we don't need to remove it
			if found {
				continue
			}

			// Otherwise, we need to remove the instance
			m.logger.Debugf("instance %s is in state %s, starting the removing process", instanceName, observedState[instanceName].GetCurrentFSMState())
			observedState[instanceName].Remove(ctx)
			continue
		}

	}

	// Now call the reconcile for each instance
	// This will only be executed if no instance was created or updated or removed
	for _, instance := range m.Instances {

		// Measure reconcile time
		start := time.Now()
		err, reconciled := instance.Reconcile(ctx)
		reconcileTime := time.Since(start)
		m.logger.Debugf("Reconcile for instance %s took %v", instance.config.Name, reconcileTime)
		if err != nil {
			return fmt.Errorf("error reconciling instance %s: %w", instance.config.Name, err), false
		}
		if reconciled {
			return nil, true
		}
	}

	return nil, false
}
