package main

import (
	"context"
	"fmt"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/control"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/logger"
)

func main() {
	// Initialize the global logger first thing
	logger.Initialize()
	defer func() {
		err := logger.Sync()
		if err != nil {
			fmt.Printf("Failed to sync logger: %v\n", err)
		}
	}()

	// Get a logger for the main component
	log := logger.For(logger.ComponentCore)

	// Log using the component logger with structured fields
	log.Info("Starting umh-core...")

	// Start the control loop
	controlLoop := control.NewControlLoop()
	controlLoop.Execute(context.Background())

	log.Info("umh-core test completed")
}
