package benthos

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	s6fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm/s6"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/s6"
)

// The functions in this file define heavier, possibly fail-prone operations
// (for example, network or file I/O) that the Benthos FSM might need to perform.
// They are intended to be called from Reconcile.
//
// IMPORTANT:
//   - Each action is expected to be idempotent, since it may be retried
//     multiple times due to transient failures.
//   - Each action takes a context.Context and can return an error if the operation fails.
//   - If an error occurs, the Reconcile function must handle
//     setting error state and scheduling a retry/backoff.

// initiateBenthosCreate attempts to create the Benthos service directory structure.
func (b *BenthosInstance) initiateBenthosCreate(ctx context.Context) error {
	b.baseFSMInstance.GetLogger().Debugf("Starting Action: Adding Benthos service %s to S6 manager ...", b.baseFSMInstance.GetID())

	// Check whether s6ServiceConfigs already contains an entry for this instance
	for _, s6Config := range b.s6ServiceConfigs {
		if s6Config.Name == b.baseFSMInstance.GetID() {
			b.baseFSMInstance.GetLogger().Debugf("Benthos service %s already exists, skipping creation", b.baseFSMInstance.GetID())
			return nil
		}
	}

	// Generate the S6 config for this instance
	s6Config, err := b.service.GenerateS6ConfigForBenthos(&b.config, b.baseFSMInstance.GetID())
	if err != nil {
		return fmt.Errorf("failed to generate S6 config for Benthos service %s: %w", b.baseFSMInstance.GetID(), err)
	}

	// Create the S6 FSM config for this instance
	s6FSMConfig := config.S6FSMConfig{
		Name:            b.baseFSMInstance.GetID(),
		DesiredState:    s6fsm.OperationalStateRunning,
		S6ServiceConfig: s6Config,
	}

	// Add the S6 FSM config to the list of S6 FSM configs
	// so that the S6 manager will start the service
	b.s6ServiceConfigs = append(b.s6ServiceConfigs, s6FSMConfig)

	b.baseFSMInstance.GetLogger().Debugf("Benthos service %s added to S6 manager", b.baseFSMInstance.GetID())
	return nil
}

// initiateBenthosRemove attempts to remove the Benthos service directory structure.
// It requires the service to be stopped before removal.
func (b *BenthosInstance) initiateBenthosRemove(ctx context.Context) error {
	b.baseFSMInstance.GetLogger().Debugf("Starting Action: Removing Benthos service %s from S6 manager ...", b.baseFSMInstance.GetID())

	// Remove the S6 FSM config from the list of S6 FSM configs
	// so that the S6 manager will stop the service
	for i, s6Config := range b.s6ServiceConfigs {
		if s6Config.Name == b.baseFSMInstance.GetID() {
			b.s6ServiceConfigs = append(b.s6ServiceConfigs[:i], b.s6ServiceConfigs[i+1:]...)
			break
		}
	}

	b.baseFSMInstance.GetLogger().Debugf("Benthos service %s removed from S6 manager", b.baseFSMInstance.GetID())
	return nil
}

// initiateBenthosStart attempts to start the Benthos service.
func (b *BenthosInstance) initiateBenthosStart(ctx context.Context) error {
	b.baseFSMInstance.GetLogger().Debugf("Starting Action: Starting Benthos service %s ...", b.baseFSMInstance.GetID())

	// TODO: Add pre-start validation

	// Start the service by setting the desired state to running for the given instance
	for i, s6Config := range b.s6ServiceConfigs {
		if s6Config.Name == b.baseFSMInstance.GetID() {
			b.s6ServiceConfigs[i].DesiredState = s6fsm.OperationalStateRunning
			break
		}
	}

	b.baseFSMInstance.GetLogger().Debugf("Benthos service %s start command executed", b.baseFSMInstance.GetID())
	return nil
}

// initiateBenthosStop attempts to stop the Benthos service.
func (b *BenthosInstance) initiateBenthosStop(ctx context.Context) error {
	b.baseFSMInstance.GetLogger().Debugf("Starting Action: Stopping Benthos service %s ...", b.baseFSMInstance.GetID())

	// Stop the service by setting the desired state to stopped for the given instance
	for i, s6Config := range b.s6ServiceConfigs {
		if s6Config.Name == b.baseFSMInstance.GetID() {
			b.s6ServiceConfigs[i].DesiredState = s6fsm.OperationalStateStopped
			break
		}
	}

	b.baseFSMInstance.GetLogger().Debugf("Benthos service %s stop command executed", b.baseFSMInstance.GetID())
	return nil
}

// updateObservedState updates the observed state of the service
func (b *BenthosInstance) updateObservedState(ctx context.Context) error {
	// Measure status time
	start := time.Now()
	info, err := b.s6Service.Status(ctx, b.servicePath)
	statusTime := time.Since(start)
	b.baseFSMInstance.GetLogger().Debugf("Status for %s took %v", b.baseFSMInstance.GetID(), statusTime)
	if err != nil {
		return err
	}

	// Lock for concurrent access
	b.stateMutex.Lock()
	defer b.stateMutex.Unlock()

	// Update service status information
	b.ObservedState.ServiceAvailable = info.WantUp // Service is available if it wants to be up
	b.ObservedState.ServiceHealthy = info.Status == s6service.ServiceUp
	b.ObservedState.ServicePID = info.Pid

	// TODO: Fetch Benthos-specific metrics and health data
	// - Collect metrics from Benthos HTTP endpoint
	// - Check logs for warnings/errors
	// - Update processing state based on throughput data

	// Fetch the actual service config from s6
	start = time.Now()
	config, err := b.s6Service.GetConfig(ctx, b.servicePath)
	configTime := time.Since(start)
	b.baseFSMInstance.GetLogger().Debugf("GetConfig for %s took %v", b.baseFSMInstance.GetID(), configTime)
	if err != nil {
		return fmt.Errorf("failed to get service config for %s: %w", b.baseFSMInstance.GetID(), err)
	}

	// TODO: Update this to check against BenthosConfig when it's available
	if !reflect.DeepEqual(config, b.config.S6ServiceConfig) {
		b.baseFSMInstance.GetLogger().Debugf("Observed config is different from desired config, triggering a re-create")
		b.logConfigDifferences(b.config.S6ServiceConfig, config)
		b.baseFSMInstance.Remove(ctx)
	}

	return nil
}

// TODO: Add additional Benthos-specific actions
// For example:
// - validateBenthosConfig - Validates Benthos configuration without starting the service
// - getBenthosMetrics - Retrieves metrics from Benthos HTTP endpoint
// - checkBenthosLogs - Analyzes logs for warnings and errors
