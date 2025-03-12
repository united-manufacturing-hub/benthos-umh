package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/agent"
)

var (
	// Command line flags
	configPath = flag.String("config", "/data/umh-lite-bootstrap.yaml", "Path to the bootstrap configuration file")
	configDir  = flag.String("config-dir", "/data/configs", "Directory for storing configuration files")
	logFile    = flag.String("log", "", "Path to the log file (empty for stdout)")
)

func main() {
	// Parse command line flags
	flag.Parse()

	// Setup logging
	var logger *log.Logger
	if *logFile != "" {
		// Log to file
		f, err := os.OpenFile(*logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open log file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		logger = log.New(f, "", log.LstdFlags)
	} else {
		// Log to stdout
		logger = log.New(os.Stdout, "", log.LstdFlags)
	}

	// Ensure config directory exists
	if err := os.MkdirAll(*configDir, 0755); err != nil {
		logger.Fatalf("Failed to create config directory: %v", err)
	}

	// Ensure the config file exists
	if _, err := os.Stat(*configPath); os.IsNotExist(err) {
		logger.Fatalf("Configuration file does not exist: %s", *configPath)
	}

	// Create absolute paths
	absConfigPath, err := filepath.Abs(*configPath)
	if err != nil {
		logger.Fatalf("Failed to get absolute path for config file: %v", err)
	}

	absConfigDir, err := filepath.Abs(*configDir)
	if err != nil {
		logger.Fatalf("Failed to get absolute path for config directory: %v", err)
	}

	// Log startup information
	logger.Printf("umh-lite-v2 starting...")
	logger.Printf("Using configuration file: %s", absConfigPath)
	logger.Printf("Using configuration directory: %s", absConfigDir)

	// Create the agent
	a, err := agent.NewAgent(absConfigPath, logger, absConfigDir)
	if err != nil {
		logger.Fatalf("Failed to create agent: %v", err)
	}

	// Set up signal handling for graceful shutdown
	agent.SetupSignalHandler(a, logger)

	// Start the agent
	if err := a.Start(); err != nil {
		logger.Fatalf("Failed to start agent: %v", err)
	}

	// Log startup success
	logger.Printf("umh-lite-v2 started successfully")

	// Keep the main goroutine alive (the agent runs in background goroutines)
	select {}
}
