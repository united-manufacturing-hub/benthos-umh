package benthos

import (
	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	public_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm"
	benthos_service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/benthos"
)

// Operational state constants (using internal_fsm compatible naming)
const (
	// OperationalStateStopped is the initial state and also the state when the service is stopped
	OperationalStateStopped = "stopped"

	// Starting phase states
	// OperationalStateStarting is the state when s6 is starting the service
	OperationalStateStarting = "starting"
	// OperationalStateStartingConfigLoading is the state when the process itself is running but is waiting for the config to be loaded
	OperationalStateStartingConfigLoading = "starting_config_loading"
	// OperationalStateStartingWaitingForHealthchecks is the state when there was no fatal config error but is waiting for the healthchecks to pass
	OperationalStateStartingWaitingForHealthchecks = "starting_waiting_for_healthchecks"
	// OperationalStateStartingWaitingForServiceToRemainRunning is the state when the service is running but is waiting for the service to remain running
	OperationalStateStartingWaitingForServiceToRemainRunning = "starting_waiting_for_service_to_remain_running"

	// Running phase states
	// OperationalStateIdle is the state when the service is running but not actively processing data
	OperationalStateIdle = "idle"
	// OperationalStateActive is the state when the service is running and actively processing data
	OperationalStateActive = "active"
	// OperationalStateDegraded is the state when the service is running but has encountered issues
	OperationalStateDegraded = "degraded"

	// OperationalStateStopping is the state when the service is in the process of stopping
	OperationalStateStopping = "stopping"
)

// Operational event constants
const (
	// Basic lifecycle events
	EventStart     = "start"
	EventStartDone = "start_done"
	EventStop      = "stop"
	EventStopDone  = "stop_done"

	// Starting phase events
	EventS6Started          = "s6_started"
	EventConfigLoaded       = "config_loaded"
	EventHealthchecksPassed = "healthchecks_passed"
	EventStartFailed        = "start_failed"

	// Running phase events
	EventDataReceived    = "data_received"
	EventNoDataTimeout   = "no_data_timeout"
	EventWarningDetected = "warning_detected"
	EventErrorDetected   = "error_detected"
	EventRecovered       = "recovered"
)

// IsOperationalState returns whether the given state is a valid operational state
func IsOperationalState(state string) bool {
	switch state {
	case OperationalStateStopped,
		OperationalStateStarting,
		OperationalStateStartingConfigLoading,
		OperationalStateStartingWaitingForHealthchecks,
		OperationalStateIdle,
		OperationalStateActive,
		OperationalStateDegraded,
		OperationalStateStopping:
		return true
	}
	return false
}

// IsStartingState returns whether the given state is a starting state
func IsStartingState(state string) bool {
	switch state {
	case OperationalStateStarting,
		OperationalStateStartingConfigLoading,
		OperationalStateStartingWaitingForHealthchecks:
		return true
	}
	return false
}

// IsRunningState returns whether the given state is a running state
func IsRunningState(state string) bool {
	switch state {
	case OperationalStateIdle,
		OperationalStateActive,
		OperationalStateDegraded:
		return true
	}
	return false
}

// BenthosObservedState contains the observed runtime state of a Benthos instance
type BenthosObservedState struct {
	ServiceAvailable bool                   `json:"serviceAvailable"` // Whether the service is available
	ServiceHealthy   bool                   `json:"serviceHealthy"`   // Whether the service is healthy
	MetricsData      map[string]interface{} `json:"metricsData"`      // Metrics data
}

// BenthosInstance implements the FSMInstance interface
// If BenthosInstance does not implement the FSMInstance interface, this will
// be detected at compile time
var _ public_fsm.FSMInstance = (*BenthosInstance)(nil)

// BenthosInstance is a state-machine managed instance of a Benthos service
type BenthosInstance struct {
	baseFSMInstance *internal_fsm.BaseFSMInstance

	// ObservedState represents the observed state of the service
	// ObservedState contains all metrics, logs, etc.
	// that are updated at the beginning of Reconcile and then used to
	// determine the next state
	ObservedState BenthosObservedState

	// service is the Benthos service implementation to use
	// It has a manager that manages the S6 service instances
	service *benthos_service.BenthosService

	// config contains all the configuration for this service
	config config.BenthosServiceConfig
}
