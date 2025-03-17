package fsm

import (
	"context"
	"fmt"
	"time"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/logger"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/metrics"
	"go.uber.org/zap"
)

// BaseFSMManager provides a generic, reusable implementation of the FSM management pattern.
// It serves as the foundation for specialized managers like S6Manager and BenthosManager,
// eliminating code duplication while allowing type-safe specialization through Go generics.
//
// Why it matters:
// - DRY (Don't Repeat Yourself): Implements common reconciliation logic once, shared across managers
// - Separation of concerns: Concrete managers only need to implement domain-specific logic
// - Standardization: Ensures consistent behavior for instance lifecycle management
// - Metrics: Provides uniform performance tracking and error reporting
// - Safety: Type parameters ensure type-safe operations while still sharing core logic
//
// How it works with generics:
// - Uses type parameter C to represent the specific configuration type (S6Config, BenthosConfig, etc.)
// - Dependency injection pattern with function callbacks for type-specific operations
// - Embedding in concrete managers through composition (S6Manager embeds BaseFSMManager[S6Config])
//
// Key responsibilities:
// - Lifecycle management: Creating, updating, and removing FSM instances
// - State reconciliation: Ensuring instances match their desired state
// - Configuration updates: Detecting and applying configuration changes
// - Error handling: Standardized error reporting and metrics collection
type BaseFSMManager[C any] struct {
	instances   map[string]FSMInstance
	logger      *zap.SugaredLogger
	managerName string

	// These methods are implemented by each concrete manager
	extractConfigs  func(config config.FullConfig) ([]C, error)
	getName         func(C) (string, error)
	getDesiredState func(C) (string, error)
	createInstance  func(C) (FSMInstance, error)
	compareConfig   func(FSMInstance, C) (bool, error)
	setConfig       func(FSMInstance, C) error
}

// NewBaseFSMManager creates a new base manager with dependencies injected.
// It follows the dependency injection pattern, where type-specific operations
// are provided as function parameters, allowing for code reuse while maintaining type safety.
//
// Parameters:
// - managerName: Identifier for metrics and logging
// - baseDir: Base directory for FSM instance files
// - extractConfigs: Extracts configuration objects of type C from the full config
// - getName: Extracts the unique name from a config object
// - getDesiredState: Determines the target state from a config object
// - createInstance: Factory function that creates appropriate FSM instances
// - compareConfig: Determines if a config change requires an update
// - setConfig: Updates an instance with new configuration
func NewBaseFSMManager[C any](
	managerName string,
	baseDir string,
	extractConfigs func(config config.FullConfig) ([]C, error),
	getName func(C) (string, error),
	getDesiredState func(C) (string, error),
	createInstance func(C) (FSMInstance, error),
	compareConfig func(FSMInstance, C) (bool, error),
	setConfig func(FSMInstance, C) error,
) *BaseFSMManager[C] {

	metrics.InitErrorCounter(metrics.ComponentBaseFSMManager, managerName)
	return &BaseFSMManager[C]{
		instances:       make(map[string]FSMInstance),
		logger:          logger.For(managerName),
		managerName:     managerName,
		extractConfigs:  extractConfigs,
		getName:         getName,
		getDesiredState: getDesiredState,
		createInstance:  createInstance,
		compareConfig:   compareConfig,
		setConfig:       setConfig,
	}
}

// GetInstances returns all instances managed by the manager.
// This provides access to the current set of running FSM instances,
// which can be useful for debugging or monitoring purposes.
func (m *BaseFSMManager[C]) GetInstances() map[string]FSMInstance {
	return m.instances
}

// GetInstance returns an instance by name.
// This allows direct access to a specific FSM instance for operations
// outside the normal reconciliation cycle.
//
// Parameters:
// - name: The unique identifier of the instance to retrieve
//
// Returns:
// - The FSMInstance if found
// - A boolean indicating whether the instance exists
func (m *BaseFSMManager[C]) GetInstance(name string) (FSMInstance, bool) {
	instance, ok := m.instances[name]
	return instance, ok
}

// AddInstanceForTest adds an instance to the manager for testing purposes.
// This method exists solely to support unit testing of managers and
// should not be used in production code.
//
// Parameters:
// - name: The unique identifier for the instance
// - instance: The FSMInstance to add to the manager
func (m *BaseFSMManager[C]) AddInstanceForTest(name string, instance FSMInstance) {
	m.instances[name] = instance
}

// GetManagerName returns the name of the manager.
// This is used for metrics reporting and logging to identify
// which manager generated a particular metric or log entry.
func (m *BaseFSMManager[C]) GetManagerName() string {
	return m.managerName
}

// Reconcile implements the core FSM management algorithm that powers the control loop.
// This method is called repeatedly by the control loop to ensure the system state
// converges toward the desired state defined in configuration.
//
// The reconciliation process follows a clear sequence:
// 1. Extract configurations for this specific manager type from the full config
// 2. For each configured instance:
//   - Create new instances if they don't exist
//   - Update configuration of existing instances if changed
//   - Update desired state if changed
//
// 3. Clean up instances that are no longer in the configuration
// 4. Reconcile each managed instance's internal state
//
// Performance metrics are collected for each phase of reconciliation,
// enabling fine-grained monitoring of system behavior.
//
// Returns:
//   - error: Any error encountered during reconciliation
//   - bool: True if a change was made, indicating the control loop should not
//     run another manager and instead should wait for the next tick
func (m *BaseFSMManager[C]) Reconcile(
	ctx context.Context,
	config config.FullConfig,
) (error, bool) {
	// Start tracking metrics for the manager
	start := time.Now()
	defer func() {
		// Record total reconcile time at the end
		metrics.ObserveReconcileTime(metrics.ComponentBaseFSMManager, m.managerName, time.Since(start))
	}()

	// Step 1: Extract the specific configs from the full config
	extractStart := time.Now()
	desiredState, err := m.extractConfigs(config)
	if err != nil {
		metrics.IncErrorCount(metrics.ComponentBaseFSMManager, m.managerName)
		return fmt.Errorf("failed to extract configs: %w", err), false
	}
	metrics.ObserveReconcileTime(metrics.ComponentBaseFSMManager, m.managerName+".extract_configs", time.Since(extractStart))

	// Step 2: Create or update instances
	for _, cfg := range desiredState {
		name, err := m.getName(cfg)
		if err != nil {
			metrics.IncErrorCount(metrics.ComponentBaseFSMManager, m.managerName)
			return fmt.Errorf("failed to get name: %w", err), false
		}

		// If the instance does not exist, create it and set it to the desired state
		if _, ok := m.instances[name]; !ok {
			createStart := time.Now()
			instance, err := m.createInstance(cfg)
			if err != nil {
				metrics.IncErrorCount(metrics.ComponentBaseFSMManager, m.managerName)
				return fmt.Errorf("failed to create instance: %w", err), false
			}
			metrics.ObserveReconcileTime(metrics.ComponentBaseFSMManager, m.managerName+".create_instance", time.Since(createStart))

			desiredState, err := m.getDesiredState(cfg)
			if err != nil {
				metrics.IncErrorCount(metrics.ComponentBaseFSMManager, m.managerName)
				return fmt.Errorf("failed to get desired state: %w", err), false
			}
			instance.SetDesiredFSMState(desiredState)
			m.instances[name] = instance
			m.logger.Infof("Created instance %s", name)
			return nil, true
		}

		// If the instance exists, but the config is different, update it
		compareStart := time.Now()
		equal, err := m.compareConfig(m.instances[name], cfg)
		if err != nil {
			metrics.IncErrorCount(metrics.ComponentBaseFSMManager, m.managerName)
			return fmt.Errorf("failed to compare config: %w", err), false
		}
		metrics.ObserveReconcileTime(metrics.ComponentBaseFSMManager, m.managerName+".compare_config", time.Since(compareStart))

		if !equal {
			updateStart := time.Now()
			err := m.setConfig(m.instances[name], cfg)
			if err != nil {
				metrics.IncErrorCount(metrics.ComponentBaseFSMManager, m.managerName)
				return fmt.Errorf("failed to set config: %w", err), false
			}
			metrics.ObserveReconcileTime(metrics.ComponentBaseFSMManager, m.managerName+".set_config", time.Since(updateStart))

			m.logger.Infof("Updated config of instance %s", name)
			return nil, true
		}

		// If the instance exists, but the desired state is different, update it
		desiredState, err := m.getDesiredState(cfg)
		if err != nil {
			metrics.IncErrorCount(metrics.ComponentBaseFSMManager, m.managerName)
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
				metrics.IncErrorCount(metrics.ComponentBaseFSMManager, m.managerName)
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
		reconcileStart := time.Now()
		err, reconciled := instance.Reconcile(ctx)
		reconcileTime := time.Since(reconcileStart)
		metrics.ObserveReconcileTime(metrics.ComponentBaseFSMManager, m.managerName+".instances."+name, reconcileTime)

		if err != nil {
			metrics.IncErrorCount(metrics.ComponentBaseFSMManager, m.managerName+".instances."+name)
			return fmt.Errorf("error reconciling instance: %w", err), false
		}
		if reconciled {
			return nil, true
		}
	}

	// Return nil if no errors occurred
	return nil, false
}
