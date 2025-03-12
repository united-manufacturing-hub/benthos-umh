package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/benthosfsm"
)

// Agent manages a collection of Benthos instances
type Agent struct {
	// Instances maps instance IDs to BenthosInstance objects
	Instances map[string]*benthosfsm.BenthosInstance

	// ServiceManager manages the actual Benthos services
	ServiceManager benthosfsm.ServiceManager

	// Events is a channel for receiving external events
	Events chan Event

	// mu protects concurrent access to the Instances map
	mu sync.RWMutex

	// wg is used to wait for all goroutines to finish during shutdown
	wg sync.WaitGroup

	// shutdown is a channel that signals shutdown when closed
	shutdown chan struct{}

	// reconcileInterval is how often to attempt reconciliation
	reconcileInterval time.Duration
}

// Event represents an external event sent to the agent
type Event struct {
	// InstanceID identifies the target instance
	InstanceID string
	// Type is the event type
	Type string
	// Payload contains additional data for the event
	Payload interface{}
}

// NewAgent creates a new Agent
func NewAgent(serviceManager benthosfsm.ServiceManager, reconcileInterval time.Duration) *Agent {
	if reconcileInterval == 0 {
		reconcileInterval = 5 * time.Second
	}

	return &Agent{
		Instances:         make(map[string]*benthosfsm.BenthosInstance),
		ServiceManager:    serviceManager,
		Events:            make(chan Event, 100),
		shutdown:          make(chan struct{}),
		reconcileInterval: reconcileInterval,
	}
}

// RegisterInstance adds a new Benthos instance to the agent
func (a *Agent) RegisterInstance(id string) (*benthosfsm.BenthosInstance, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, exists := a.Instances[id]; exists {
		return nil, fmt.Errorf("instance %s already exists", id)
	}

	instance := benthosfsm.NewBenthosInstance(id, nil)
	benthosfsm.RegisterCallbacks(instance, a.ServiceManager)
	a.Instances[id] = instance

	return instance, nil
}

// GetInstance retrieves a Benthos instance by ID
func (a *Agent) GetInstance(id string) (*benthosfsm.BenthosInstance, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	instance, exists := a.Instances[id]
	return instance, exists
}

// UpdateInstanceConfig sets a new desired configuration for an instance
func (a *Agent) UpdateInstanceConfig(id string, content string) error {
	instance, exists := a.GetInstance(id)
	if !exists {
		return fmt.Errorf("instance %s does not exist", id)
	}

	// Create a new BenthosConfig from the content
	config := benthosfsm.NewBenthosConfig(content)

	// Set the config path
	configPath, err := a.ServiceManager.WriteConfig(id, content)
	if err != nil {
		return err
	}
	config.SetConfigPath(configPath)

	// Set the desired config on the instance (use a method to avoid directly accessing unexported fields)
	return a.setInstanceConfig(instance, config)
}

// setInstanceConfig is a helper method to set the configuration on an instance
func (a *Agent) setInstanceConfig(instance *benthosfsm.BenthosInstance, config *benthosfsm.BenthosConfig) error {
	// This is a simplified implementation - in a real implementation we'd likely need to use exported methods
	// on BenthosInstance to handle setting configs properly

	// For now, we'll just signal a config change if the instance is in the running state
	if instance.GetState() == benthosfsm.StateRunning {
		// Trigger a config change event
		ctx := context.Background()
		return instance.SendEvent(ctx, benthosfsm.EventConfigChange)
	}

	return nil
}

// Start starts the agent's control loop
func (a *Agent) Start(ctx context.Context) {
	a.wg.Add(1)
	go a.controlLoop(ctx)
}

// Stop stops the agent's control loop
func (a *Agent) Stop() {
	close(a.shutdown)
	a.wg.Wait()
}

// SendEvent sends an event to the agent
func (a *Agent) SendEvent(event Event) {
	select {
	case a.Events <- event:
		// Event sent successfully
	default:
		// Channel full, could log an error here
	}
}

// SetInstanceDesiredState sets the desired state for an instance
func (a *Agent) SetInstanceDesiredState(id string, state string) error {
	instance, exists := a.GetInstance(id)
	if !exists {
		return fmt.Errorf("instance %s does not exist", id)
	}

	instance.SetDesiredState(state)
	return nil
}

// StartInstance sets an instance's desired state to running
func (a *Agent) StartInstance(id string) error {
	return a.SetInstanceDesiredState(id, benthosfsm.StateRunning)
}

// StopInstance sets an instance's desired state to stopped
func (a *Agent) StopInstance(id string) error {
	return a.SetInstanceDesiredState(id, benthosfsm.StateStopped)
}

// GetAllInstances returns a map of all instance IDs to their current states
func (a *Agent) GetAllInstances() map[string]string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	states := make(map[string]string)
	for id, instance := range a.Instances {
		states[id] = instance.GetState()
	}

	return states
}

// GetRunningInstances returns a list of IDs for instances in the running state
func (a *Agent) GetRunningInstances() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var running []string
	for id, instance := range a.Instances {
		if instance.GetState() == benthosfsm.StateRunning {
			running = append(running, id)
		}
	}

	return running
}

// handleEvent processes an external event
func (a *Agent) handleEvent(ctx context.Context, event Event) error {
	// Check if the instance exists without capturing the instance variable
	_, exists := a.GetInstance(event.InstanceID)
	if !exists {
		return fmt.Errorf("instance %s does not exist", event.InstanceID)
	}

	switch event.Type {
	case "start":
		return a.StartInstance(event.InstanceID)
	case "stop":
		return a.StopInstance(event.InstanceID)
	case "update_config":
		if content, ok := event.Payload.(string); ok {
			return a.UpdateInstanceConfig(event.InstanceID, content)
		}
		return fmt.Errorf("invalid payload for update_config event")
	default:
		return fmt.Errorf("unknown event type: %s", event.Type)
	}
}

// ReconcileAll performs reconciliation for all instances
func (a *Agent) ReconcileAll(ctx context.Context) {
	a.mu.RLock()
	instances := make([]*benthosfsm.BenthosInstance, 0, len(a.Instances))
	for _, instance := range a.Instances {
		instances = append(instances, instance)
	}
	a.mu.RUnlock()

	for _, instance := range instances {
		// Run reconciliation in a goroutine to avoid blocking
		a.wg.Add(1)
		go func(inst *benthosfsm.BenthosInstance) {
			defer a.wg.Done()
			err := benthosfsm.ReconcileInstance(ctx, inst, a.ServiceManager)
			if err != nil {
				// Log error or take other action
			}
		}(instance)
	}
}

// controlLoop is the main control loop for the agent
func (a *Agent) controlLoop(ctx context.Context) {
	defer a.wg.Done()

	ticker := time.NewTicker(a.reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-a.shutdown:
			return
		case <-ticker.C:
			// Perform periodic reconciliation
			a.ReconcileAll(ctx)
		case event := <-a.Events:
			// Handle external events
			err := a.handleEvent(ctx, event)
			if err != nil {
				// Log error or take other action
			}
		}
	}
}
