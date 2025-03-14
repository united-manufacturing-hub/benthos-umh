package benthos

import (
	"context"
	"fmt"
	"reflect"
	"time"

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
	b.baseFSMInstance.GetLogger().Debugf("Starting Action: Creating Benthos service %s ...", b.baseFSMInstance.GetID())

	// TODO: Generate Benthos configuration from the BenthosConfig
	// This will include setting up inputs, processors, outputs, etc.

	// For now, using S6 service creation with default config
	// This will be expanded to include Benthos-specific configuration
	err := b.s6Service.Create(ctx, b.servicePath, s6service.S6ServiceConfig{
		Command: []string{"benthos", "--version"}, // Placeholder command
		Env: map[string]string{
			"BENTHOS_LOG_LEVEL": "debug",
		},
		// TODO: Add ConfigFiles for Benthos configuration
	})
	if err != nil {
		return fmt.Errorf("failed to create service for %s: %w", b.baseFSMInstance.GetID(), err)
	}

	b.baseFSMInstance.GetLogger().Debugf("Benthos service %s directory structure created", b.baseFSMInstance.GetID())
	return nil
}

// initiateBenthosRemove attempts to remove the Benthos service directory structure.
// It requires the service to be stopped before removal.
func (b *BenthosInstance) initiateBenthosRemove(ctx context.Context) error {
	b.baseFSMInstance.GetLogger().Debugf("Starting Action: Removing Benthos service %s ...", b.baseFSMInstance.GetID())

	// First ensure the service is stopped
	if b.IsBenthosRunning() {
		return fmt.Errorf("service %s cannot be removed while running", b.baseFSMInstance.GetID())
	}

	// Remove the service directory
	err := b.s6Service.Remove(ctx, b.servicePath)
	if err != nil {
		return fmt.Errorf("failed to remove service directory for %s: %w", b.baseFSMInstance.GetID(), err)
	}

	b.baseFSMInstance.GetLogger().Debugf("Benthos service %s removed", b.baseFSMInstance.GetID())
	return nil
}

// initiateBenthosStart attempts to start the Benthos service.
func (b *BenthosInstance) initiateBenthosStart(ctx context.Context) error {
	b.baseFSMInstance.GetLogger().Debugf("Starting Action: Starting Benthos service %s ...", b.baseFSMInstance.GetID())

	// TODO: Add pre-start validation and configuration updates if needed

	err := b.s6Service.Start(ctx, b.servicePath)
	if err != nil {
		return fmt.Errorf("failed to start Benthos service %s: %w", b.baseFSMInstance.GetID(), err)
	}

	b.baseFSMInstance.GetLogger().Debugf("Benthos service %s start command executed", b.baseFSMInstance.GetID())
	return nil
}

// initiateBenthosStop attempts to stop the Benthos service.
func (b *BenthosInstance) initiateBenthosStop(ctx context.Context) error {
	b.baseFSMInstance.GetLogger().Debugf("Starting Action: Stopping Benthos service %s ...", b.baseFSMInstance.GetID())

	err := b.s6Service.Stop(ctx, b.servicePath)
	if err != nil {
		return fmt.Errorf("failed to stop Benthos service %s: %w", b.baseFSMInstance.GetID(), err)
	}

	b.baseFSMInstance.GetLogger().Debugf("Benthos service %s stop command executed", b.baseFSMInstance.GetID())
	return nil
}

// initiateBenthosRestart attempts to restart the Benthos service.
func (b *BenthosInstance) initiateBenthosRestart(ctx context.Context) error {
	b.baseFSMInstance.GetLogger().Debugf("Starting Action: Restarting Benthos service %s ...", b.baseFSMInstance.GetID())

	err := b.s6Service.Restart(ctx, b.servicePath)
	if err != nil {
		return fmt.Errorf("failed to restart Benthos service %s: %w", b.baseFSMInstance.GetID(), err)
	}

	b.baseFSMInstance.GetLogger().Debugf("Benthos service %s restart command executed", b.baseFSMInstance.GetID())
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

	// Store the raw service info
	b.ObservedState.S6ServiceInfo = info

	// TODO: Fetch Benthos-specific metrics and health data
	// - Collect metrics from Benthos HTTP endpoint
	// - Check logs for warnings/errors
	// - Update processing state based on throughput data

	// Set LastStateChange time if this is the first update
	if b.ObservedState.LastStateChange == 0 {
		b.ObservedState.LastStateChange = time.Now().Unix()
	}

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

// IsBenthosRunning checks if the Benthos service is running.
func (b *BenthosInstance) IsBenthosRunning() bool {
	return b.ObservedState.S6ServiceInfo.Status == s6service.ServiceUp
}

// IsBenthosStopped checks if the Benthos service is stopped.
func (b *BenthosInstance) IsBenthosStopped() bool {
	return b.ObservedState.S6ServiceInfo.Status == s6service.ServiceDown
}

// logConfigDifferences logs the specific differences between desired and observed configurations
func (b *BenthosInstance) logConfigDifferences(desired, observed s6service.S6ServiceConfig) {
	b.baseFSMInstance.GetLogger().Infof("Configuration differences for %s:", b.baseFSMInstance.GetID())

	// Command differences
	if !reflect.DeepEqual(desired.Command, observed.Command) {
		b.baseFSMInstance.GetLogger().Infof("Command - want: %v", desired.Command)
		b.baseFSMInstance.GetLogger().Infof("Command - is:   %v", observed.Command)
	}

	// Environment variables differences
	if !reflect.DeepEqual(desired.Env, observed.Env) {
		b.baseFSMInstance.GetLogger().Infof("Environment variables differences:")

		// Check for keys in desired that are missing or different in observed
		for k, v := range desired.Env {
			if observedVal, ok := observed.Env[k]; !ok {
				b.baseFSMInstance.GetLogger().Infof("   - %s: want: %q, is: <missing>", k, v)
			} else if v != observedVal {
				b.baseFSMInstance.GetLogger().Infof("   - %s: want: %q, is: %q", k, v, observedVal)
			}
		}

		// Check for keys in observed that are not in desired
		for k, v := range observed.Env {
			if _, ok := desired.Env[k]; !ok {
				b.baseFSMInstance.GetLogger().Infof("   - %s: want: <missing>, is: %q", k, v)
			}
		}
	}

	// Config files differences
	if !reflect.DeepEqual(desired.ConfigFiles, observed.ConfigFiles) {
		b.baseFSMInstance.GetLogger().Infof("Config files differences:")
		// Implementation details for config file comparison would go here
	}
}

// TODO: Add additional Benthos-specific actions
// For example:
// - validateBenthosConfig - Validates Benthos configuration without starting the service
// - getBenthosMetrics - Retrieves metrics from Benthos HTTP endpoint
// - checkBenthosLogs - Analyzes logs for warnings and errors
