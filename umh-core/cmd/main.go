package main

import (
	"context"
	"fmt"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/control"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/logger"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/metrics"
)

func main() {
	// Initialize the global logger first thing
	logger.Initialize()

	// Get a logger for the main component
	log := logger.For(logger.ComponentCore)

	// Log using the component logger with structured fields
	log.Info("Starting umh-core...")

	// Load the config
	configManager := config.NewFileConfigManager()
	config, err := configManager.GetConfig(context.Background(), 0)
	if err != nil {
		log.Fatalf("Failed to load config: %s", err)
	}

	// Start the metrics server
	server := metrics.SetupMetricsEndpoint(fmt.Sprintf(":%d", config.Agent.MetricsPort))
	defer server.Shutdown(context.Background())

	// Start the control loop
	controlLoop := control.NewControlLoop()
	controlLoop.Execute(context.Background())

	log.Info("umh-core test completed")
}
