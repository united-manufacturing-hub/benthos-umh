package benthos

import (
	"context"
	"log"
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
//     setting BenthosInstance.lastError and scheduling a retry/backoff.

// InitiateServiceCreation attempts to create the S6 service for this Benthos instance.
func (b *BenthosInstance) InitiateServiceCreation(ctx context.Context) error {
	log.Printf("Starting Action: Creating S6 service for Benthos instance %s ...", b.ID)

	// In the future, this will:
	// 1. Create the S6 service configuration
	// 2. Register the service with S6
	// 3. Set up any necessary directories or files
	// 4. Initialize the service in a stopped state

	return nil
}

// InitiateServiceRemoval attempts to remove the S6 service for this Benthos instance.
func (b *BenthosInstance) InitiateServiceRemoval(ctx context.Context) error {
	log.Printf("Starting Action: Removing S6 service for Benthos instance %s ...", b.ID)

	// In the future, this will:
	// 1. Stop the service if it's running
	// 2. Unregister the service from S6
	// 3. Clean up any created files or directories
	// 4. Remove service configuration

	return nil
}

// InitiateBenthosStart attempts to start the Benthos process or service.
func (b *BenthosInstance) InitiateBenthosStart(ctx context.Context) error {
	// Example of a fail-prone operation: e.g. run a system command, spawn a container, etc.
	// This is just a placeholder.
	log.Printf("Starting Action: Starting Benthos instance %s ...", b.ID)

	// Simulate a possible transient failure:
	// if err := someExternalStartLogic(...); err != nil {
	//     return fmt.Errorf("failed to start Benthos: %w", err)
	// }

	return nil
}

// InitiateBenthosStop attempts to stop the Benthos process or service.
func (b *BenthosInstance) InitiateBenthosStop(ctx context.Context) error {
	log.Printf("Starting Action: Stopping Benthos instance %s ...", b.ID)

	// For example:
	// if err := someExternalStopLogic(...); err != nil {
	//     return fmt.Errorf("failed to stop Benthos: %w", err)
	// }
	return nil
}

// IsBenthosRunning checks if the Benthos instance is running.
// Through healthchecks, etc.
func (b *BenthosInstance) IsBenthosRunning() bool {
	return b.ObservedState.IsRunning
}

// IsBenthosStopped checks if the Benthos instance is stopped.
// Through healthchecks, etc.
func (b *BenthosInstance) IsBenthosStopped() bool {
	return !b.ObservedState.IsRunning
}
