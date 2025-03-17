package benthos

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

// BenthosManager implements FSM management for Benthos services.
type BenthosManager struct {
	public_fsm.FSMManager
	Instances map[string]*BenthosInstance
	logger    *zap.SugaredLogger

	// TODO: Add a port manager to allocate unique ports for Benthos metrics endpoints
	// portManager PortManager
}

func NewBenthosManager() *BenthosManager {
	return &BenthosManager{
		Instances: make(map[string]*BenthosInstance),
		logger:    logger.For(logger.ComponentBenthosManager),
	}
}

const (
	baseBenthosDir = "/run/service" // Same as s6 for now
)

// Reconcile reconciles desired & observed states for Benthos services.
// It returns a boolean indicating if the manager was reconciled.
func (m *BenthosManager) Reconcile(ctx context.Context, desiredState []config.BenthosConfig) (error, bool) {

	// Step 1: Detect external changes
	observedState := m.Instances

	// Step 2: Create or update instances
	for _, instance := range desiredState {

		// If the instance does not exist, create it and set it to the desired state
		if _, ok := observedState[instance.Name]; !ok {
			observedState[instance.Name] = NewBenthosInstance(baseBenthosDir, instance)
			observedState[instance.Name].SetDesiredFSMState(instance.DesiredState)
			m.logger.Infof("Created Benthos instance %s", instance.Name)
			return nil, true
		}

		// If the instance exists, but the config is different, update it
		// TODO: Implement proper comparison for Benthos config
		// if !observedState[instance.Name].config.S6ServiceConfig.Equal(instance.S6ServiceConfig) {
		// 	observedState[instance.Name].config = instance
		// 	m.logger.Infof("Updated config of Benthos instance %s", instance.Name)
		// 	return nil, true
		// }

		// If the instance exists, but the desired state is different, update it
		if observedState[instance.Name].GetDesiredFSMState() != instance.DesiredState {
			m.logger.Infof("Updated desired state of Benthos instance %s from %s to %s",
				instance.Name,
				observedState[instance.Name].GetDesiredFSMState(),
				instance.DesiredState)
			observedState[instance.Name].SetDesiredFSMState(instance.DesiredState)
			return nil, true
		}
	}

	// Step 3: Clean up any instances that are not in desiredState, or are in the removed state
	for instanceName := range observedState {
		// Check if instance is in the desired state list
		found := false
		for _, desired := range desiredState {
			if desired.Name == instanceName {
				found = true
				break
			}
		}

		switch observedState[instanceName].GetCurrentFSMState() {
		case internal_fsm.LifecycleStateRemoving:
			m.logger.Debugf("Benthos instance %s is already in removing state, waiting until it is removed", instanceName)
			continue
		case internal_fsm.LifecycleStateRemoved:
			m.logger.Debugf("Benthos instance %s is in removed state, deleting it from the manager", instanceName)
			delete(observedState, instanceName)
			continue
		default:
			// If the instance is in desiredState, we don't need to remove it
			if found {
				continue
			}

			// Otherwise, we need to remove the instance
			m.logger.Debugf("Benthos instance %s is in state %s, starting the removing process",
				instanceName,
				observedState[instanceName].GetCurrentFSMState())
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
		m.logger.Debugf("Reconcile for Benthos instance %s took %v", instance.baseFSMInstance.GetID(), reconcileTime)
		if err != nil {
			return fmt.Errorf("error reconciling Benthos instance %s: %w",
				instance.baseFSMInstance.GetID(), err), false
		}
		if reconciled {
			return nil, true
		}
	}

	return nil, false
}

// TODO: Add a PortManager for allocating unique ports for Benthos metrics endpoints
// type PortManager interface {
//     AllocatePort(instanceName string) (int, error)
//     ReleasePort(instanceName string) error
//     GetPort(instanceName string) (int, bool)
// }
