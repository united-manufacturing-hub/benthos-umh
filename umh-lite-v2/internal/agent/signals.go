package agent

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

// SetupSignalHandler sets up a handler for OS signals to allow graceful shutdown
func SetupSignalHandler(agent *Agent, logger *log.Logger) {
	// Channel to receive OS signals
	sigs := make(chan os.Signal, 1)

	// Register for SIGINT and SIGTERM
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Start a goroutine to handle signals
	go func() {
		sig := <-sigs
		logger.Printf("Received signal: %v", sig)
		logger.Printf("Initiating graceful shutdown...")

		// Stop the agent
		agent.Stop()

		// Exit with success status code
		os.Exit(0)
	}()
}
