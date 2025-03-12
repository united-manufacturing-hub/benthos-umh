package benthosfsm

import (
	"context"
	"sync"
	"time"

	"github.com/looplab/fsm"
)

// State constants represent the various states a Benthos instance can be in
const (
	// StateStopped indicates the instance is not running
	StateStopped = "stopped"
	// StateStarting indicates the instance is in the process of starting
	StateStarting = "starting"
	// StateRunning indicates the instance is running normally
	StateRunning = "running"
	// StateConfigChanged indicates a configuration change has been detected
	StateConfigChanged = "config_changed"
	// StateChangingConfig indicates the instance is applying a new configuration
	StateChangingConfig = "changing_config"
	// StateConfigError indicates a configuration change has failed
	StateConfigError = "config_error"
	// StateError indicates the instance has encountered an error
	StateError = "error"
)

// Event constants represent the events that can trigger state transitions
const (
	// EventStart is triggered to start an instance
	EventStart = "start"
	// EventStarted is triggered when an instance has successfully started
	EventStarted = "started"
	// EventStop is triggered to stop an instance
	EventStop = "stop"
	// EventStopped is triggered when an instance has successfully stopped
	EventStopped = "stopped"
	// EventConfigChange is triggered when a configuration change is detected
	EventConfigChange = "config_change"
	// EventUpdateConfig is triggered to update an instance's configuration
	EventUpdateConfig = "update_config"
	// EventUpdateDone is triggered when a configuration update is complete
	EventUpdateDone = "update_done"
	// EventConfigError is triggered when a configuration update fails
	EventConfigError = "config_error"
	// EventRollback is triggered to roll back to a previous configuration
	EventRollback = "rollback"
	// EventRollbackDone is triggered when a rollback is complete
	EventRollbackDone = "rollback_done"
	// EventFail is triggered when an error occurs
	EventFail = "fail"
	// EventRetry is triggered to retry after an error
	EventRetry = "retry"
	// EventShutdown is triggered to shut down the agent
	EventShutdown = "shutdown"
)

// BenthosConfig represents a Benthos configuration
type BenthosConfig struct {
	// ConfigPath is the path to the configuration file
	ConfigPath string
	// Content is the actual configuration content
	Content string
	// Version is a version identifier for the configuration (hash or timestamp)
	Version string
	// CreateTime is when the configuration was created
	CreateTime time.Time
	// UpdateTime is when the configuration was last updated
	UpdateTime time.Time
}

// ConfigHistoryEntry represents a historical configuration entry with metadata
type ConfigHistoryEntry struct {
	// Config is the historical configuration
	Config *BenthosConfig
	// AppliedAt is when this configuration was applied
	AppliedAt time.Time
	// Status indicates if this config was successful (success, error, unknown)
	Status string
}

// ConfigHistoryStatus defines possible status values for config history entries
const (
	ConfigStatusSuccess = "success"
	ConfigStatusError   = "error"
	ConfigStatusUnknown = "unknown"
)

// MaxHistorySize defines the maximum number of historical configurations to keep
const MaxHistorySize = 20

// BenthosInstance represents a single Benthos pipeline instance with a state machine
type BenthosInstance struct {
	// ID is a unique identifier for this instance
	ID string

	// Mutex for protecting concurrent access to fields
	mu sync.RWMutex

	// FSM is the finite state machine that manages instance state
	FSM *fsm.FSM

	// DesiredState represents the target state we want to reach
	DesiredState string

	// CurrentConfig is the currently applied configuration
	CurrentConfig *BenthosConfig
	// DesiredConfig is the new configuration to be applied
	DesiredConfig *BenthosConfig
	// ConfigHistory stores the history of previously applied configurations
	ConfigHistory []ConfigHistoryEntry

	// LastTransition is the time of the last state transition
	LastTransition time.Time
	// LastError stores the most recent error, if any
	LastError error
	// LastConfigError stores the most recent configuration error
	LastConfigError error

	// RetryCount for transient errors
	RetryCount int
	// NextRetry is the time for the next retry attempt
	NextRetry time.Time

	// Process is the handle to the Benthos process (implementation-specific)
	// This could be a *exec.Cmd, a reference to a plugin instance, etc.
	Process interface{}

	// Callbacks for state transitions
	callbacks map[string]fsm.Callback
}

// NewBenthosInstance creates a new BenthosInstance with the given ID
func NewBenthosInstance(id string, callbacks map[string]fsm.Callback) *BenthosInstance {
	if callbacks == nil {
		callbacks = make(map[string]fsm.Callback)
	}

	instance := &BenthosInstance{
		ID:             id,
		DesiredState:   StateStopped,
		LastTransition: time.Now(),
		callbacks:      callbacks,
		ConfigHistory:  make([]ConfigHistoryEntry, 0, MaxHistorySize),
	}

	// Define the FSM transitions
	instance.FSM = fsm.NewFSM(
		StateStopped, // Initial state
		fsm.Events{
			// Stopped -> Starting -> Running
			{Name: EventStart, Src: []string{StateStopped, StateError}, Dst: StateStarting},
			{Name: EventStarted, Src: []string{StateStarting}, Dst: StateRunning},

			// Running -> Stopped
			{Name: EventStop, Src: []string{StateRunning, StateError, StateConfigError}, Dst: StateStopped},

			// Config change flow
			{Name: EventConfigChange, Src: []string{StateRunning}, Dst: StateConfigChanged},
			{Name: EventUpdateConfig, Src: []string{StateConfigChanged}, Dst: StateChangingConfig},
			{Name: EventUpdateDone, Src: []string{StateChangingConfig}, Dst: StateStarting},

			// Config error and rollback flow
			{Name: EventConfigError, Src: []string{StateChangingConfig, StateStarting}, Dst: StateConfigError},
			{Name: EventRollback, Src: []string{StateConfigError}, Dst: StateChangingConfig},
			{Name: EventRollbackDone, Src: []string{StateChangingConfig}, Dst: StateStarting},

			// Error handling
			{Name: EventFail, Src: []string{StateStarting, StateRunning, StateChangingConfig}, Dst: StateError},
			{Name: EventRetry, Src: []string{StateError, StateConfigError}, Dst: StateStopped},
		},
		fsm.Callbacks{
			"enter_state": func(ctx context.Context, e *fsm.Event) {
				instance.mu.Lock()
				instance.LastTransition = time.Now()
				instance.mu.Unlock()

				// Call registered callback for this state if exists
				if cb, ok := instance.callbacks["enter_"+e.Dst]; ok {
					cb(ctx, e)
				}
			},
		},
	)

	return instance
}

// GetState safely returns the current state
func (b *BenthosInstance) GetState() string {
	return b.FSM.Current()
}

// SetDesiredState safely updates the desired state
func (b *BenthosInstance) SetDesiredState(state string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.DesiredState = state
}

// GetDesiredState safely returns the desired state
func (b *BenthosInstance) GetDesiredState() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.DesiredState
}

// SetError records an error encountered by the instance
func (b *BenthosInstance) SetError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.LastError = err
	if err != nil {
		b.RetryCount++
		// Set a backoff time based on retry count (exponential backoff)
		backoff := time.Duration(1<<uint(b.RetryCount-1)) * time.Second
		if backoff > 1*time.Minute {
			backoff = 1 * time.Minute // Cap at 1 minute
		}
		b.NextRetry = time.Now().Add(backoff)
	}
}

// GetError safely returns the last error
func (b *BenthosInstance) GetError() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.LastError
}

// ClearError resets error state
func (b *BenthosInstance) ClearError() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.LastError = nil
	b.RetryCount = 0
}

// ShouldRetry returns true if it's time to retry after an error
func (b *BenthosInstance) ShouldRetry() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.LastError != nil && time.Now().After(b.NextRetry)
}

// SendEvent sends an event to the FSM and returns whether the event was processed
func (b *BenthosInstance) SendEvent(ctx context.Context, eventName string, args ...interface{}) error {
	return b.FSM.Event(ctx, eventName, args...)
}

// CanTransition checks if a transition is possible from the current state
func (b *BenthosInstance) CanTransition(eventName string) bool {
	return b.FSM.Can(eventName)
}

// GetCurrentConfig safely returns the current configuration
func (b *BenthosInstance) GetCurrentConfig() *BenthosConfig {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.CurrentConfig
}

// SetCurrentConfig safely updates the current configuration
func (b *BenthosInstance) SetCurrentConfig(config *BenthosConfig) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.CurrentConfig = config
}

// GetDesiredConfig safely returns the desired configuration
func (b *BenthosInstance) GetDesiredConfig() *BenthosConfig {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.DesiredConfig
}

// SetDesiredConfig safely updates the desired configuration
func (b *BenthosInstance) SetDesiredConfig(config *BenthosConfig) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.DesiredConfig = config
}

// NeedsConfigChange returns true if the instance has a new desired config that differs from the current config
// This is used by the reconciliation loop to determine if a config change is needed
func (b *BenthosInstance) NeedsConfigChange() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.CurrentConfig != nil && b.DesiredConfig != nil && !b.DesiredConfig.Equal(b.CurrentConfig)
}

// SetNextRetryTime safely updates the next retry time
func (b *BenthosInstance) SetNextRetryTime(t time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.NextRetry = t
}

// AddToConfigHistory adds a configuration to the history with the given status
func (b *BenthosInstance) AddToConfigHistory(config *BenthosConfig, status string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Create a deep copy of the config to prevent mutation
	configCopy := *config

	entry := ConfigHistoryEntry{
		Config:    &configCopy,
		AppliedAt: time.Now(),
		Status:    status,
	}

	// Add to the beginning of the slice (newest first)
	b.ConfigHistory = append([]ConfigHistoryEntry{entry}, b.ConfigHistory...)

	// Trim if exceeding max size
	if len(b.ConfigHistory) > MaxHistorySize {
		b.ConfigHistory = b.ConfigHistory[:MaxHistorySize]
	}
}

// GetConfigHistory returns a copy of the configuration history
func (b *BenthosInstance) GetConfigHistory() []ConfigHistoryEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Create a copy to prevent external mutation
	historyCopy := make([]ConfigHistoryEntry, len(b.ConfigHistory))
	copy(historyCopy, b.ConfigHistory)

	return historyCopy
}

// GetLatestSuccessfulConfig returns the most recent successfully applied configuration
func (b *BenthosInstance) GetLatestSuccessfulConfig() *BenthosConfig {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, entry := range b.ConfigHistory {
		if entry.Status == ConfigStatusSuccess {
			// Return a copy to prevent mutation
			configCopy := *entry.Config
			return &configCopy
		}
	}

	return nil
}

// SetConfigError records a configuration error
func (b *BenthosInstance) SetConfigError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.LastConfigError = err
}

// GetConfigError safely returns the last configuration error
func (b *BenthosInstance) GetConfigError() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.LastConfigError
}

// ClearConfigError resets the configuration error state
func (b *BenthosInstance) ClearConfigError() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.LastConfigError = nil
}
