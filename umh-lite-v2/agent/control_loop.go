package agent

import (
	"context"
	"log"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/benthosfsm"
)

// Control loop operational modes
const (
	// ModeActive means the control loop actively reconciles instance states
	ModeActive = "active"
	// ModePassive means the control loop only reacts to external events
	ModePassive = "passive"
	// ModeShutdown means the control loop is shutting down
	ModeShutdown = "shutdown"
)

// ControlLoopConfig contains configuration for the control loop
type ControlLoopConfig struct {
	// Mode is the operational mode (active or passive)
	Mode string
	// ReconcileInterval is how often to reconcile in active mode
	ReconcileInterval time.Duration
	// MaxConcurrentReconciles is the maximum number of concurrent reconciliations
	MaxConcurrentReconciles int
	// ShutdownTimeout is the timeout for graceful shutdown
	ShutdownTimeout time.Duration
}

// DefaultControlLoopConfig returns the default control loop configuration
func DefaultControlLoopConfig() ControlLoopConfig {
	return ControlLoopConfig{
		Mode:                    ModeActive,
		ReconcileInterval:       5 * time.Second,
		MaxConcurrentReconciles: 5,
		ShutdownTimeout:         30 * time.Second,
	}
}

// RunControlLoop starts the control loop with the given configuration
func (a *Agent) RunControlLoop(ctx context.Context, config ControlLoopConfig) error {
	a.wg.Add(1)
	defer a.wg.Done()

	// Create a ticker for periodic reconciliation
	var ticker *time.Ticker
	if config.Mode == ModeActive {
		ticker = time.NewTicker(config.ReconcileInterval)
		defer ticker.Stop()
	}

	// Semaphore to limit concurrent reconciliations
	sem := make(chan struct{}, config.MaxConcurrentReconciles)

	// Main control loop
	for {
		select {
		case <-ctx.Done():
			// Context canceled, proceed to shutdown
			return a.shutdownLoop(config.ShutdownTimeout)

		case <-a.shutdown:
			// Explicit shutdown request
			return a.shutdownLoop(config.ShutdownTimeout)

		case event := <-a.Events:
			// Handle incoming event
			log.Printf("Received event: %s for instance %s", event.Type, event.InstanceID)
			err := a.handleEvent(ctx, event)
			if err != nil {
				log.Printf("Error handling event: %v", err)
			}

		case <-ticker.C:
			// Skip reconciliation if not in active mode
			if config.Mode != ModeActive {
				continue
			}

			// Get a snapshot of instances to reconcile
			instances := a.getInstancesSnapshot()

			// Reconcile each instance
			for id, instance := range instances {
				// Skip if at max concurrent reconciliations
				select {
				case sem <- struct{}{}: // Acquire semaphore
				default:
					log.Printf("Max concurrent reconciliations reached, skipping instance %s", id)
					continue
				}

				// Launch reconciliation goroutine
				a.wg.Add(1)
				go func(id string, inst *benthosfsm.BenthosInstance) {
					defer a.wg.Done()
					defer func() { <-sem }() // Release semaphore

					log.Printf("Reconciling instance %s (state: %s, desired: %s)",
						id, inst.GetState(), inst.GetDesiredState())

					err := benthosfsm.ReconcileInstance(ctx, inst, a.ServiceManager)
					if err != nil {
						log.Printf("Error reconciling instance %s: %v", id, err)
					}
				}(id, instance)
			}
		}
	}
}

// getInstancesSnapshot returns a copy of the current instances map
func (a *Agent) getInstancesSnapshot() map[string]*benthosfsm.BenthosInstance {
	a.mu.RLock()
	defer a.mu.RUnlock()

	snapshot := make(map[string]*benthosfsm.BenthosInstance, len(a.Instances))
	for id, instance := range a.Instances {
		snapshot[id] = instance
	}

	return snapshot
}

// shutdownLoop performs a graceful shutdown of the control loop
func (a *Agent) shutdownLoop(timeout time.Duration) error {
	log.Printf("Initiating graceful shutdown (timeout: %v)", timeout)

	// Create a context with timeout for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Get all running instances
	running := a.GetRunningInstances()
	log.Printf("Stopping %d running instances", len(running))

	// Stop all running instances
	for _, id := range running {
		// Use the context when stopping instances to respect the timeout
		err := a.stopInstanceWithContext(ctx, id)
		if err != nil {
			log.Printf("Error stopping instance %s: %v", id, err)
		}
	}

	// Wait for all instances to stop or timeout
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		running = a.GetRunningInstances()
		if len(running) == 0 {
			log.Printf("All instances stopped successfully")
			return nil
		}

		log.Printf("Waiting for %d instances to stop", len(running))
		time.Sleep(500 * time.Millisecond)
	}

	log.Printf("Shutdown timeout reached, %d instances still running", len(a.GetRunningInstances()))
	return nil
}

// stopInstanceWithContext stops an instance using the provided context
func (a *Agent) stopInstanceWithContext(ctx context.Context, id string) error {
	// Set the desired state
	err := a.SetInstanceDesiredState(id, benthosfsm.StateStopped)
	if err != nil {
		return err
	}

	// Get the instance
	instance, exists := a.GetInstance(id)
	if !exists {
		return nil // Instance no longer exists, consider it stopped
	}

	// Trigger an immediate stop event
	return instance.SendEvent(ctx, benthosfsm.EventStop)
}
