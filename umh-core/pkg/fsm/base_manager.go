package fsm

import (
	"context"
	"fmt"
	"time"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	"go.uber.org/zap"
)

// / BaseFSMManager provides a generic implementation for FSM managers
type BaseFSMManager[C any] struct {
	instances map[string]FSMInstance
	logger    *zap.SugaredLogger

	// These methods are implemented by each concrete manager
	extractConfigs  func(config config.FullConfig) ([]C, error)
	getName         func(C) (string, error)
	getDesiredState func(C) (string, error)
	createInstance  func(C) (FSMInstance, error)
	compareConfig   func(FSMInstance, C) (bool, error)
	setConfig       func(FSMInstance, C) error
}

// NewBaseFSMManager creates a new base manager with dependencies injected
func NewBaseFSMManager[C any](
	logger *zap.SugaredLogger,
	baseDir string,
	extractConfigs func(config config.FullConfig) ([]C, error),
	getName func(C) (string, error),
	getDesiredState func(C) (string, error),
	createInstance func(C) (FSMInstance, error),
	compareConfig func(FSMInstance, C) (bool, error),
	setConfig func(FSMInstance, C) error,
) *BaseFSMManager[C] {
	return &BaseFSMManager[C]{
		instances:       make(map[string]FSMInstance),
		logger:          logger,
		extractConfigs:  extractConfigs,
		getName:         getName,
		getDesiredState: getDesiredState,
		createInstance:  createInstance,
		compareConfig:   compareConfig,
		setConfig:       setConfig,
	}
}

// GetInstances returns all instances managed by the manager
func (m *BaseFSMManager[C]) GetInstances() map[string]FSMInstance {
	return m.instances
}

// GetInstance returns an instance by name
func (m *BaseFSMManager[C]) GetInstance(name string) (FSMInstance, bool) {
	instance, ok := m.instances[name]
	return instance, ok
}

// AddInstanceForTest adds an instance to the manager for testing purposes
func (m *BaseFSMManager[C]) AddInstanceForTest(name string, instance FSMInstance) {
	m.instances[name] = instance
}

// ReconcileManager implements common FSM management logic
func (m *BaseFSMManager[C]) Reconcile(
	ctx context.Context,
	config config.FullConfig,
) (error, bool) {

	// Step 1: Extract the specific configs from the full config
	desiredState, err := m.extractConfigs(config)
	if err != nil {
		return fmt.Errorf("failed to extract configs: %w", err), false
	}

	// Step 2: Create or update instances
	for _, cfg := range desiredState {
		name, err := m.getName(cfg)
		if err != nil {
			return fmt.Errorf("failed to get name: %w", err), false
		}

		// If the instance does not exist, create it and set it to the desired state
		if _, ok := m.instances[name]; !ok {
			instance, err := m.createInstance(cfg)
			if err != nil {
				return fmt.Errorf("failed to create instance: %w", err), false
			}
			desiredState, err := m.getDesiredState(cfg)
			if err != nil {
				return fmt.Errorf("failed to get desired state: %w", err), false
			}
			instance.SetDesiredFSMState(desiredState)
			m.instances[name] = instance
			m.logger.Infof("Created instance %s", name)
			return nil, true
		}

		// If the instance exists, but the config is different, update it
		equal, err := m.compareConfig(m.instances[name], cfg)
		if err != nil {
			return fmt.Errorf("failed to compare config: %w", err), false
		}
		if !equal {
			err := m.setConfig(m.instances[name], cfg)
			if err != nil {
				return fmt.Errorf("failed to set config: %w", err), false
			}
			m.logger.Infof("Updated config of instance %s", name)
			return nil, true
		}

		// If the instance exists, but the desired state is different, update it
		desiredState, err := m.getDesiredState(cfg)
		if err != nil {
			return fmt.Errorf("failed to get desired state: %w", err), false
		}
		if m.instances[name].GetDesiredFSMState() != desiredState {
			m.logger.Infof("Updated desired state of instance %s from %s to %s",
				name, m.instances[name].GetDesiredFSMState(), desiredState)
			m.instances[name].SetDesiredFSMState(desiredState)
			return nil, true
		}
	}

	// Step 3: Clean up any instances that are not in desiredState, or are in the removed state
	// Before deletion, they need to be gracefully stopped and we need to wait until they are in the state removed
	for instanceName := range m.instances {

		// If the instance is not in desiredState, start its removal process
		found := false
		for _, desired := range desiredState {
			name, err := m.getName(desired)
			if err != nil {
				return fmt.Errorf("failed to get name: %w", err), false
			}
			if name == instanceName {
				found = true
				break
			}
		}

		switch m.instances[instanceName].GetCurrentFSMState() {
		case internal_fsm.LifecycleStateRemoving:
			m.logger.Debugf("instance %s is already in removing state, waiting until it is removed", instanceName)
			continue
		case internal_fsm.LifecycleStateRemoved:
			m.logger.Debugf("instance %s is in removed state, deleting it from the manager", instanceName)
			delete(m.instances, instanceName)
			continue
		default:
			// If the instance is in desiredState, we don't need to remove it
			if found {
				continue
			}

			// Otherwise, we need to remove the instance
			m.logger.Debugf("instance %s is in state %s, starting the removing process", instanceName, m.instances[instanceName].GetCurrentFSMState())
			m.instances[instanceName].Remove(ctx)
			continue
		}

	}

	// Reconcile instances
	for name, instance := range m.instances {
		start := time.Now()
		err, reconciled := instance.Reconcile(ctx)
		reconcileTime := time.Since(start)
		m.logger.Debugf("Reconcile for instance %s took %v", name, reconcileTime)
		if err != nil {
			return fmt.Errorf("error reconciling instance: %w", err), false
		}
		if reconciled {
			return nil, true
		}
	}

	return nil, false
}
