package main

import (
	"context"
	"log"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/fsm/s6"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/service/s6"
)

const (
	s6BaseDir = "/run/service"
)

func main() {
	log.Println("Starting umh-lite-v2...")

	// Create a benthos service with config
	benthosConfig := s6service.ServiceConfig{
		Command: []string{"/usr/local/bin/benthos", "-c", "/benthos.yaml"},
		Env: map[string]string{
			"LOG_LEVEL": "DEBUG",
		},
		ConfigFiles: map[string]string{
			// This is a path relative to service dir - will be created in /etc/s6-overlay/s6-rc.d/benthos/config/example.txt
			"config/example.txt": "This is an example config file\n",
			// This is an absolute path
			"/benthos.yaml": `---
http:
  address: 0.0.0.0:4195
logger:
  level: ${LOG_LEVEL}
  format: json
`,
		},
	}

	benthosService := s6.NewServiceInBaseDir(
		"benthos",     // Service name
		s6BaseDir,     // Base directory
		benthosConfig, // Service configuration
		nil,           // Callbacks (optional)
	)

	ctx := context.Background()
	log.Println("Created S6 instances for services")

	// Now test the benthos service
	log.Println("Testing benthos service...")

	// Set desired state to running
	err := benthosService.SetDesiredFSMState(s6.OperationalStateRunning)
	if err != nil {
		log.Fatalf("Failed to set desired state to running: %v", err)
	}

	// Reconcile and monitor the service startup
	for i := 0; i < 5; i++ {
		err := benthosService.Reconcile(ctx)
		if err != nil {
			log.Printf("Error reconciling benthos service: %s", err)
		}
		log.Printf("Benthos service - Iteration %d - Current state: %s, Observed state: %+v",
			i,
			benthosService.GetCurrentFSMState(),
			benthosService.ObservedState,
		)
		time.Sleep(2 * time.Second)
	}

	// Test stopping both services
	log.Println("Testing service stop...")

	// Stop benthos service
	err = benthosService.SetDesiredFSMState(s6.OperationalStateStopped)
	if err != nil {
		log.Printf("Error setting benthos service to stopped state: %s", err)
	}

	// Monitor the stopping process
	for i := 0; i < 5; i++ {
		// Reconcile benthos service
		err = benthosService.Reconcile(ctx)
		if err != nil {
			log.Printf("Error reconciling benthos service during stop: %s", err)
		}

		log.Printf("Stop iteration %d - Benthos service state: %s",
			i,
			benthosService.GetCurrentFSMState(),
		)

		// Check if benthos service has stopped
		if benthosService.GetCurrentFSMState() == s6.OperationalStateStopped {
			log.Println("All services successfully stopped")
			break
		}

		time.Sleep(2 * time.Second)
	}

	log.Println("umh-lite-v2 test completed")
}
