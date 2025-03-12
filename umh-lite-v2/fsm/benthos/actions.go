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
	// TODO: Implement this
	return true
}

// IsBenthosStopped checks if the Benthos instance is stopped.
// Through healthchecks, etc.
func (b *BenthosInstance) IsBenthosStopped() bool {
	// TODO: Implement this
	return true
}
