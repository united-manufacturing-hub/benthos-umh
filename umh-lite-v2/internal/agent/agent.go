package agent

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/benthos"
	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/config"
	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/models"
)

// Constants for the agent
const (
	// DefaultTickerInterval is the default interval for the control loop ticker
	DefaultTickerInterval = 100 * time.Millisecond

	// DefaultConfigCheckInterval is how often to check for config file changes
	DefaultConfigCheckInterval = 5 * time.Second

	// DefaultOperationTimeout is the default timeout for operations
	DefaultOperationTimeout = 10 * time.Second

	// MaxEventBufferSize is the maximum size of the event buffer
	MaxEventBufferSize = 100
)

// Agent represents the main control agent for the umh-lite-v2 system
type Agent struct {
	// Mutex for protecting shared state
	mu sync.RWMutex

	// Instances stores all Benthos instances
	Instances map[string]*models.BenthosInstance

	// Events is the channel for incoming events
	Events chan models.Event

	// ConfigPath is the path to the configuration file
	ConfigPath string

	// BenthosManager manages Benthos processes
	BenthosManager *benthos.Manager

	// TickerInterval is the interval for the control loop ticker
	TickerInterval time.Duration

	// ConfigCheckInterval is how often to check for config file changes
	ConfigCheckInterval time.Duration

	// Watcher watches for config file changes
	Watcher *config.ConfigWatcher

	// Context and cancel function for the control loop
	ctx    context.Context
	cancel context.CancelFunc

	// WaitGroup for graceful shutdown
	wg sync.WaitGroup

	// Logger for agent operations
	logger *log.Logger
}

// NewAgent creates a new Agent
func NewAgent(configPath string, logger *log.Logger, configDir string) (*Agent, error) {
	benthosManager, err := benthos.NewManager(configDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create benthos manager: %w", err)
	}

	watcher, err := config.NewConfigWatcher(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create config watcher: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Agent{
		Instances:           make(map[string]*models.BenthosInstance),
		Events:              make(chan models.Event, MaxEventBufferSize),
		ConfigPath:          configPath,
		BenthosManager:      benthosManager,
		TickerInterval:      DefaultTickerInterval,
		ConfigCheckInterval: DefaultConfigCheckInterval,
		Watcher:             watcher,
		ctx:                 ctx,
		cancel:              cancel,
		logger:              logger,
	}, nil
}

// Start starts the agent
func (a *Agent) Start() error {
	// Load initial config
	if err := a.loadConfig(); err != nil {
		return fmt.Errorf("failed to load initial config: %w", err)
	}

	// Start control loop
	a.wg.Add(1)
	go a.loop()

	// Start config watcher
	a.wg.Add(1)
	go a.watchConfig()

	return nil
}

// Stop stops the agent
func (a *Agent) Stop() {
	a.cancel()
	a.wg.Wait()
}

// loadConfig loads the configuration from the config file
func (a *Agent) loadConfig() error {
	cfg, err := config.LoadConfig(a.ConfigPath)
	if err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Create or update instances based on configuration
	for _, benthosConfig := range cfg.Benthos {
		instance, exists := a.Instances[benthosConfig.ID]

		if !exists {
			// Create new instance
			instance = models.NewBenthosInstance(benthosConfig.ID)
			a.Instances[benthosConfig.ID] = instance
		}

		// Update instance configuration
		desiredConfig := &models.BenthosConfig{
			ConfigPath: "", // Will be set when written to disk
			Content:    benthosConfig.Config,
			Version:    time.Now().Format(time.RFC3339Nano),
		}

		instance.DesiredConfig = desiredConfig

		// Set desired state based on enabled flag
		if benthosConfig.Enabled {
			instance.SetDesiredState(models.StateRunning)
		} else {
			instance.SetDesiredState(models.StateStopped)
		}
	}

	// Find instances that no longer exist in config
	for id, instance := range a.Instances {
		found := false
		for _, benthosConfig := range cfg.Benthos {
			if benthosConfig.ID == id {
				found = true
				break
			}
		}

		if !found {
			// Set instance to stop if it no longer exists in config
			instance.SetDesiredState(models.StateStopped)
		}
	}

	return nil
}

// watchConfig periodically checks for changes to the config file
func (a *Agent) watchConfig() {
	defer a.wg.Done()

	ticker := time.NewTicker(a.ConfigCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			changed, err := a.Watcher.CheckForChanges()
			if err != nil {
				a.logger.Printf("Error checking for config changes: %v", err)
				continue
			}

			if changed {
				a.logger.Printf("Configuration file changed, reloading")
				if err := a.loadConfig(); err != nil {
					a.logger.Printf("Error reloading config: %v", err)
				}
			}
		case <-a.ctx.Done():
			return
		}
	}
}

// loop is the main control loop
func (a *Agent) loop() {
	defer a.wg.Done()

	ticker := time.NewTicker(a.TickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Periodic reconciliation
			a.reconcileInstances()
		case event := <-a.Events:
			// Handle incoming events
			a.handleEvent(event)
		case <-a.ctx.Done():
			// Graceful shutdown
			a.logger.Printf("Shutting down agent control loop")
			return
		}
	}
}

// reconcileInstances updates the state of all instances
func (a *Agent) reconcileInstances() {
	a.mu.RLock()
	// Make a copy of instance IDs to avoid holding the lock during processing
	var instanceIDs []string
	for id := range a.Instances {
		instanceIDs = append(instanceIDs, id)
	}
	a.mu.RUnlock()

	for _, id := range instanceIDs {
		a.reconcileInstance(id)
	}
}

// reconcileInstance updates the state of a single instance
func (a *Agent) reconcileInstance(id string) {
	a.mu.RLock()
	instance, exists := a.Instances[id]
	if !exists {
		a.mu.RUnlock()
		return
	}

	// Make local copies of state to minimize lock time
	currentState := instance.GetState()
	a.mu.RUnlock()

	// Create a context with timeout for operations
	ctx, cancel := context.WithTimeout(a.ctx, DefaultOperationTimeout)
	defer cancel()

	// Take action based on current state
	switch currentState {
	case models.StateStopped:
		a.handleStoppedState(ctx, instance)
	case models.StateStarting:
		a.handleStartingState(ctx, instance)
	case models.StateRunning:
		a.handleRunningState(ctx, instance)
	case models.StateConfigChanged:
		a.handleConfigChangedState(ctx, instance)
	case models.StateChangingConfig:
		a.handleChangingConfigState(ctx, instance)
	case models.StateError:
		a.handleErrorState(ctx, instance)
	}
}

// handleStoppedState handles an instance in the stopped state
func (a *Agent) handleStoppedState(ctx context.Context, instance *models.BenthosInstance) {
	a.mu.RLock()
	desiredState := instance.DesiredState
	a.mu.RUnlock()

	if desiredState == models.StateRunning {
		// Transition to Starting
		a.logger.Printf("Instance %s: Stopped -> Starting", instance.ID)
		instance.SetState(models.StateStarting)
	}
}

// handleStartingState handles an instance in the starting state
func (a *Agent) handleStartingState(ctx context.Context, instance *models.BenthosInstance) {
	a.mu.Lock()
	// Check if we need to write configuration
	if instance.DesiredConfig != nil && instance.CurrentConfig != instance.DesiredConfig {
		// Write the configuration file
		configPath, err := a.BenthosManager.WriteConfig(instance.ID, instance.DesiredConfig.Content)
		if err != nil {
			a.logger.Printf("Instance %s: Failed to write config: %v", instance.ID, err)
			instance.SetError(err)
			instance.SetState(models.StateError)
			a.mu.Unlock()
			return
		}

		// Update the config path
		instance.DesiredConfig.ConfigPath = configPath
		instance.CurrentConfig = instance.DesiredConfig
	}

	configPath := ""
	if instance.CurrentConfig != nil {
		configPath = instance.CurrentConfig.ConfigPath
	}
	a.mu.Unlock()

	if configPath == "" {
		a.logger.Printf("Instance %s: No configuration path available", instance.ID)
		err := fmt.Errorf("no configuration path available")
		instance.SetError(err)
		instance.SetState(models.StateError)
		return
	}

	// Start the Benthos process
	if err := a.BenthosManager.Start(ctx, instance.ID, configPath); err != nil {
		a.logger.Printf("Instance %s: Failed to start: %v", instance.ID, err)
		instance.SetError(err)
		instance.SetState(models.StateError)
		return
	}

	// Transition to Running
	a.logger.Printf("Instance %s: Starting -> Running", instance.ID)
	instance.SetState(models.StateRunning)
	instance.ClearError()
}

// handleRunningState handles an instance in the running state
func (a *Agent) handleRunningState(ctx context.Context, instance *models.BenthosInstance) {
	a.mu.RLock()
	desiredState := instance.DesiredState
	desiredConfig := instance.DesiredConfig
	currentConfig := instance.CurrentConfig
	a.mu.RUnlock()

	// Check if the process is actually running
	isRunning := a.BenthosManager.IsRunning(instance.ID)
	if !isRunning {
		a.logger.Printf("Instance %s: Process not running but state is Running", instance.ID)
		instance.SetState(models.StateStopped)
		return
	}

	// Check if config has changed
	if desiredConfig != currentConfig {
		a.logger.Printf("Instance %s: Configuration change detected", instance.ID)
		instance.SetState(models.StateConfigChanged)
		return
	}

	// Check if we need to stop
	if desiredState == models.StateStopped {
		a.logger.Printf("Instance %s: Stopping instance", instance.ID)
		if err := a.BenthosManager.Stop(ctx, instance.ID); err != nil {
			a.logger.Printf("Instance %s: Failed to stop: %v", instance.ID, err)
			instance.SetError(err)
			instance.SetState(models.StateError)
			return
		}

		a.logger.Printf("Instance %s: Running -> Stopped", instance.ID)
		instance.SetState(models.StateStopped)
	}
}

// handleConfigChangedState handles an instance with changed configuration
func (a *Agent) handleConfigChangedState(ctx context.Context, instance *models.BenthosInstance) {
	a.logger.Printf("Instance %s: ConfigChanged -> ChangingConfig", instance.ID)
	instance.SetState(models.StateChangingConfig)
}

// handleChangingConfigState handles an instance that is changing configuration
func (a *Agent) handleChangingConfigState(ctx context.Context, instance *models.BenthosInstance) {
	a.mu.RLock()
	desiredConfig := instance.DesiredConfig
	a.mu.RUnlock()

	if desiredConfig == nil {
		a.logger.Printf("Instance %s: No desired config available", instance.ID)
		instance.SetError(fmt.Errorf("no desired config available"))
		instance.SetState(models.StateError)
		return
	}

	// Update the Benthos process with new configuration
	if err := a.BenthosManager.Update(ctx, instance.ID, desiredConfig.Content); err != nil {
		a.logger.Printf("Instance %s: Failed to update config: %v", instance.ID, err)
		instance.SetError(err)
		instance.SetState(models.StateError)
		return
	}

	// Update current config
	a.mu.Lock()
	instance.CurrentConfig = desiredConfig
	a.mu.Unlock()

	// Transition to Starting state to complete the update
	a.logger.Printf("Instance %s: ChangingConfig -> Starting", instance.ID)
	instance.SetState(models.StateStarting)
}

// handleErrorState handles an instance in the error state
func (a *Agent) handleErrorState(ctx context.Context, instance *models.BenthosInstance) {
	// Check if we should retry
	if !instance.ShouldRetry() {
		return
	}

	a.logger.Printf("Instance %s: Retrying after error (attempt %d)", instance.ID, instance.RetryCount)

	// Transition to Stopped state and retry from there
	instance.SetState(models.StateStopped)
}

// handleEvent processes an event from the event channel
func (a *Agent) handleEvent(event models.Event) {
	a.logger.Printf("Received event: %s for instance %s", event.Type, event.InstanceID)

	a.mu.RLock()
	instance, exists := a.Instances[event.InstanceID]
	a.mu.RUnlock()

	if !exists {
		a.logger.Printf("Received event for unknown instance: %s", event.InstanceID)
		return
	}

	switch event.Type {
	case models.EventStart:
		instance.SetDesiredState(models.StateRunning)

	case models.EventStop:
		instance.SetDesiredState(models.StateStopped)

	case models.EventRestart:
		// To restart, first set desired state to stopped, then in next iteration to running
		instance.SetDesiredState(models.StateStopped)
		// Queue up a start event after a short delay
		go func() {
			time.Sleep(500 * time.Millisecond)
			a.Events <- models.NewEvent(event.InstanceID, models.EventStart, nil)
		}()

	case models.EventUpdateConfig:
		if config, ok := event.Payload.(string); ok {
			a.mu.Lock()
			instance.DesiredConfig = &models.BenthosConfig{
				ConfigPath: "", // Will be set when written to disk
				Content:    config,
				Version:    time.Now().Format(time.RFC3339Nano),
			}
			a.mu.Unlock()

			// If already running, mark config as changed
			if instance.GetState() == models.StateRunning {
				instance.SetState(models.StateConfigChanged)
			}
		} else {
			a.logger.Printf("Invalid payload for UpdateConfig event")
		}
	}
}
