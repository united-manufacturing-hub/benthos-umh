package benthos

import (
	"context"
	"fmt"

	s6fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm/s6"
	benthos_service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/benthos"
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

// initiateBenthosCreate attempts to add the Benthos to the S6 manager.
func (b *BenthosInstance) initiateBenthosCreate(ctx context.Context) error {
	b.baseFSMInstance.GetLogger().Debugf("Starting Action: Adding Benthos service %s to S6 manager ...", b.baseFSMInstance.GetID())

	err := b.service.AddBenthosToS6Manager(ctx, &b.config, b.baseFSMInstance.GetID())
	if err != nil {
		if err == benthos_service.ErrServiceAlreadyExists {
			b.baseFSMInstance.GetLogger().Debugf("Benthos service %s already exists in S6 manager", b.baseFSMInstance.GetID())
			return nil // do not throw an error, as each action is expected to be idempotent
		}
		return fmt.Errorf("failed to add Benthos service %s to S6 manager: %w", b.baseFSMInstance.GetID(), err)
	}

	b.baseFSMInstance.GetLogger().Debugf("Benthos service %s added to S6 manager", b.baseFSMInstance.GetID())
	return nil
}

// initiateBenthosRemove attempts to remove the Benthos from the S6 manager.
// It requires the service to be stopped before removal.
func (b *BenthosInstance) initiateBenthosRemove(ctx context.Context) error {
	b.baseFSMInstance.GetLogger().Debugf("Starting Action: Removing Benthos service %s from S6 manager ...", b.baseFSMInstance.GetID())

	// Remove the Benthos from the S6 manager
	err := b.service.RemoveBenthosFromS6Manager(ctx, b.baseFSMInstance.GetID())
	if err != nil {
		if err == benthos_service.ErrServiceNotExist {
			b.baseFSMInstance.GetLogger().Debugf("Benthos service %s not found in S6 manager", b.baseFSMInstance.GetID())
			return nil // do not throw an error, as each action is expected to be idempotent
		}
		return fmt.Errorf("failed to remove Benthos service %s from S6 manager: %w", b.baseFSMInstance.GetID(), err)
	}

	b.baseFSMInstance.GetLogger().Debugf("Benthos service %s removed from S6 manager", b.baseFSMInstance.GetID())
	return nil
}

// initiateBenthosStart attempts to start the benthos by setting the desired state to running for the given instance
func (b *BenthosInstance) initiateBenthosStart(ctx context.Context) error {
	b.baseFSMInstance.GetLogger().Debugf("Starting Action: Starting Benthos service %s ...", b.baseFSMInstance.GetID())

	// TODO: Add pre-start validation

	// Set the desired state to running for the given instance
	err := b.service.StartBenthos(ctx, b.baseFSMInstance.GetID())
	if err != nil {
		// if the service is not there yet but we attempt to start it, we need to throw an error
		return fmt.Errorf("failed to start Benthos service %s: %w", b.baseFSMInstance.GetID(), err)
	}

	b.baseFSMInstance.GetLogger().Debugf("Benthos service %s start command executed", b.baseFSMInstance.GetID())
	return nil
}

// initiateBenthosStop attempts to stop the Benthos by setting the desired state to stopped for the given instance
func (b *BenthosInstance) initiateBenthosStop(ctx context.Context) error {
	b.baseFSMInstance.GetLogger().Debugf("Starting Action: Stopping Benthos service %s ...", b.baseFSMInstance.GetID())

	// Set the desired state to stopped for the given instance
	err := b.service.StopBenthos(ctx, b.baseFSMInstance.GetID())
	if err != nil {
		// if the service is not there yet but we attempt to stop it, we need to throw an error
		return fmt.Errorf("failed to stop Benthos service %s: %w", b.baseFSMInstance.GetID(), err)
	}

	b.baseFSMInstance.GetLogger().Debugf("Benthos service %s stop command executed", b.baseFSMInstance.GetID())
	return nil
}

// updateObservedState updates the observed state of the service
func (b *BenthosInstance) updateObservedState(ctx context.Context, tick uint64) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	info, err := b.service.Status(ctx, b.baseFSMInstance.GetID(), b.config.MetricsPort, tick)
	if err != nil {
		// TODO: Handle this error
		return err
	}

	// Store the raw service info
	b.ObservedState.ServiceInfo = info

	// NOTE: Unlike S6Instance, we don't need to check for config reconciliation here.
	// This is because:
	// 1. Config reconciliation is already handled at the S6Instance level
	// 2. BenthosInstance doesn't interact directly with the filesystem
	// 3. When we modify s6ServiceConfigs, those changes propagate to the S6Manager
	//    which handles creating/updating the actual S6Instances
	// 4. Any config drift would be detected by the corresponding S6Instance's
	//    updateObservedState method, making the check here redundant
	// Instead, this method should focus on Benthos-specific metrics and health monitoring.
	return nil
}

// IsBenthosS6Running determines if the Benthos S6 FSM is in running state.
// Architecture Decision: We intentionally rely only on the FSM state, not the underlying
// service implementation details. This maintains a clean separation of concerns where:
// 1. The FSM is the source of truth for service state
// 2. We trust the FSM's state management completely
// 3. Implementation details of how S6 determines running state are encapsulated away
func (b *BenthosInstance) IsBenthosS6Running() bool {
	return b.ObservedState.ServiceInfo.S6FSMState == s6fsm.OperationalStateRunning
}

// IsBenthosS6Stopped determines if the Benthos S6 FSM is in stopped state.
// We follow the same architectural principle as IsBenthosS6Running - relying solely
// on the FSM state to maintain clean separation of concerns.
func (b *BenthosInstance) IsBenthosS6Stopped() bool {
	return b.ObservedState.ServiceInfo.S6FSMState == s6fsm.OperationalStateStopped
}

// IsBenthosConfigLoaded determines if the Benthos service has successfully loaded its configuration.
// Implementation: We check if the service has been running for at least 5 seconds without crashing.
// This works because Benthos performs config validation at startup and immediately panics
// if there are any configuration errors, causing the service to restart.
// Therefore, if the service stays up for >= 5 seconds, we can be confident the config is valid.
func (b *BenthosInstance) IsBenthosConfigLoaded() bool {
	currentUptime := b.ObservedState.ServiceInfo.S6ObservedState.ServiceInfo.Uptime
	return currentUptime >= 5
}

// IsBenthosHealthchecksPassed determines if the Benthos service has passed its healthchecks.
func (b *BenthosInstance) IsBenthosHealthchecksPassed() bool {
	return b.ObservedState.ServiceInfo.BenthosStatus.HealthCheck.IsLive &&
		b.ObservedState.ServiceInfo.BenthosStatus.HealthCheck.IsReady
}

// IsBenthosRunningForSomeTime determines if the Benthos service has been running for some time.
func (b *BenthosInstance) IsBenthosRunningForSomeTime() bool {
	currentUptime := b.ObservedState.ServiceInfo.S6ObservedState.ServiceInfo.Uptime
	if currentUptime < 10 {
		return false
	}

	return true
}

// IsBenthosLogsFine determines if there are any issues in the Benthos logs
func (b *BenthosInstance) IsBenthosLogsFine() bool {
	return b.service.IsLogsFine(b.ObservedState.ServiceInfo.BenthosStatus.Logs)
}

// IsBenthosMetricsErrorFree determines if the Benthos service has no errors in the metrics
func (b *BenthosInstance) IsBenthosMetricsErrorFree() bool {
	return b.service.IsMetricsErrorFree(b.ObservedState.ServiceInfo.BenthosStatus.Metrics)
}

// IsBenthosDegraded determines if the Benthos service is degraded.
// These check everything that is checked during the starting phase
// But it means that it once worked, and then degraded
func (b *BenthosInstance) IsBenthosDegraded() bool {
	if b.IsBenthosS6Running() && b.IsBenthosConfigLoaded() && b.IsBenthosHealthchecksPassed() && b.IsBenthosRunningForSomeTime() {
		return false
	}
	return true
}

// IsBenthosWithProcessingActivity determines if the Benthos instance has active data processing
// based on metrics data and possibly other observed state information
func (b *BenthosInstance) IsBenthosWithProcessingActivity() bool {
	if b.ObservedState.ServiceInfo.BenthosStatus.MetricsState == nil {
		return false
	}
	return b.service.HasProcessingActivity(b.ObservedState.ServiceInfo.BenthosStatus)
}

// TODO: Add additional Benthos-specific actions
// For example:
// - validateBenthosConfig - Validates Benthos configuration without starting the service
// - getBenthosMetrics - Retrieves metrics from Benthos HTTP endpoint
// - checkBenthosLogs - Analyzes logs for warnings and errors
