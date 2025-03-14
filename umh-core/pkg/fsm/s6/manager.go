package s6

import (
	"context"
	"log"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	public_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm"
)

// S6Manager implements FSM management for S6 services.
type S6Manager struct {
	public_fsm.FSMManager
	Instances map[string]*S6Instance
}

func NewS6Manager() *S6Manager {
	return &S6Manager{
		Instances: make(map[string]*S6Instance),
	}
}

const (
	baseS6Dir = "/run/service"
)

// Reconcile reconciles desired & observed states for S6 services.
func (m *S6Manager) Reconcile(ctx context.Context, cfg config.FullConfig) error {

	// Step 1: Detect external changes
	desiredState := cfg.Services
	observedState := m.Instances

	// Step 2: Create or update instances
	for _, instance := range desiredState {

		// If the instance does not exist, create it and set it to the desired state
		if _, ok := observedState[instance.Name]; !ok {
			observedState[instance.Name] = NewS6Instance(baseS6Dir, instance)
			observedState[instance.Name].SetDesiredFSMState(instance.DesiredState)
			log.Printf("[S6Manager] created instance %s", instance.Name)
			return nil
		}

		// If the instance exists, but the config is different, update it
		if !observedState[instance.Name].config.S6ServiceConfig.Equal(instance.S6ServiceConfig) {
			observedState[instance.Name].config = instance
			log.Printf("[S6Manager] updated config of instance %s", instance.Name)
			return nil
		}

		// If the instance exists, but the desired state is different, update it
		if observedState[instance.Name].GetDesiredFSMState() != instance.DesiredState {
			observedState[instance.Name].SetDesiredFSMState(instance.DesiredState)
			log.Printf("[S6Manager] updated desired state of instance %s from %s to %s", instance.Name, observedState[instance.Name].GetDesiredFSMState(), instance.DesiredState)
			return nil
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
			log.Printf("[S6Manager] instance %s is already in removing state, waiting until it is removed", instanceName)
			continue
		case internal_fsm.LifecycleStateRemoved:
			log.Printf("[S6Manager] instance %s is in removed state, deleting it from the manager", instanceName)
			delete(observedState, instanceName)
			continue
		default:
			// If the instance is in desiredState, we don't need to remove it
			if found {
				continue
			}

			// Otherwise, we need to remove the instance
			log.Printf("[S6Manager] instance %s is in state %s, starting the removing process", instanceName, observedState[instanceName].GetCurrentFSMState())
			observedState[instanceName].Remove(ctx)
			continue
		}

	}

	// Now call the reconcile for each instance
	// This will only be executed if no instance was created or updated or removed
	for _, instance := range m.Instances {
		instance.Reconcile(ctx)
	}

	return nil
}
