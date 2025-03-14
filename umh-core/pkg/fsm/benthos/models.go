package benthos

import (
	"time"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	public_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/s6"
)

// Operational state constants represent the runtime states of a Benthos service
const (
	// Basic operational states (similar to S6)
	OperationalStateStarting = "starting"
	OperationalStateRunning  = "running"
	OperationalStateStopping = "stopping"
	OperationalStateStopped  = "stopped"

	// Benthos-specific states
	// TODO: Finalize the actual state machine design
	// These are potential additional states that might be useful
	OperationalStateConfigLoading          = "config_loading"
	OperationalStateConfigInvalid          = "config_invalid"
	OperationalStateInitializingComponents = "initializing_components"
	OperationalStateProcessing             = "processing"
	OperationalStateProcessingWithWarnings = "processing_with_warnings"
	OperationalStateProcessingWithErrors   = "processing_with_errors"
	OperationalStateIdle                   = "idle" // When running but not processing data
)

func IsOperationalState(state string) bool {
	switch state {
	case OperationalStateStopped,
		OperationalStateStarting,
		OperationalStateRunning,
		OperationalStateStopping,
		OperationalStateConfigLoading,
		OperationalStateConfigInvalid,
		OperationalStateInitializingComponents,
		OperationalStateProcessing,
		OperationalStateProcessingWithWarnings,
		OperationalStateProcessingWithErrors,
		OperationalStateIdle:
		return true
	default:
		return false
	}
}

// Operational event constants represent events related to service runtime states
const (
	// Basic operational events (similar to S6)
	EventStart     = "start"
	EventStartDone = "start_done"
	EventStop      = "stop"
	EventStopDone  = "stop_done"
	EventFail      = "fail"
	EventRestart   = "restart"

	// Benthos-specific events
	// TODO: Finalize the event list based on the state machine design
	EventConfigValid            = "config_valid"
	EventConfigInvalid          = "config_invalid"
	EventComponentsInitialized  = "components_initialized"
	EventDataReceived           = "data_received"
	EventDataProcessingComplete = "data_processing_complete"
	EventWarningDetected        = "warning_detected"
	EventErrorDetected          = "error_detected"
	EventNoDataTimeout          = "no_data_timeout"
)

// BenthosObservedState represents the state of the Benthos service as observed externally
type BenthosObservedState struct {
	// Last time the state was observed to change
	LastStateChange int64

	// Underlying S6 service info
	S6ServiceInfo s6service.ServiceInfo

	// Benthos-specific observed state
	// TODO: Determine what Benthos-specific metrics and state to track
	MetricsData map[string]interface{}

	// Health information
	LastRestartTime       time.Time
	RecentRestarts        int
	WarningsCount         int
	ErrorsCount           int
	IsProcessingData      bool
	LastDataProcessedTime time.Time

	// Configuration validation state
	ConfigValid  bool
	ConfigErrors []string
}

// BenthosConfig represents the configuration for a Benthos service
// This should be defined in the config package eventually
// TODO: Move this to config package
type BenthosConfig struct {
	// Basic service configuration
	Name         string
	DesiredState string

	// Benthos-specific configuration
	Inputs     []map[string]interface{}
	Processors []map[string]interface{}
	Outputs    []map[string]interface{}
	Caches     []map[string]interface{}
	RateLimits []map[string]interface{}
	Buffers    []map[string]interface{}

	// Advanced configuration
	MetricsPort       int // Unique port for metrics HTTP server
	LogLevel          string
	HTTPServerEnabled bool
}

// BenthosInstance implements the FSMInstance interface
// If BenthosInstance does not implement the FSMInstance interface, this will
// be detected at compile time
var _ public_fsm.FSMInstance = (*BenthosInstance)(nil)

// BenthosInstance represents a single Benthos service instance with a state machine
type BenthosInstance struct {
	// The base FSM instance that handles common FSM functionality
	baseFSMInstance *internal_fsm.BaseFSMInstance

	// ObservedState represents the observed state of the service
	// Contains all metrics, logs, etc. that are updated during reconciliation
	ObservedState BenthosObservedState

	// servicePath is the path to the s6 service directory
	servicePath string

	// Underlying S6 service that will be used to manage the Benthos process
	s6Service s6service.Service

	// Configuration for the Benthos instance
	// TODO: Update this when the config package has a BenthosConfig type
	config config.S6FSMConfig // This will be replaced with BenthosConfig

	// BenthosService interface would be added here once implemented
	// benthosService BenthosService
}
