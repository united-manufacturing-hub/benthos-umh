package agent

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/benthos"
	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/models"
)

// TestAgentStateTransitions tests the basic state transitions
func TestAgentStateTransitions(t *testing.T) {
	// Create temporary directories for testing
	testDir, err := ioutil.TempDir("", "umh-lite-test")
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	configDir := filepath.Join(testDir, "configs")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("Failed to create config directory: %v", err)
	}

	// Create a sample configuration file
	configPath := filepath.Join(testDir, "umh-lite-test.yaml")
	sampleConfig := `
apiKey: "test-api-key"
metadata:
  deviceId: "test-device"
  location: "test.location"
externalMQTT:
  enabled: false
  broker: "tcp://localhost:1883"
logLevel: "info"
benthos:
  - id: "test-instance"
    type: "dummy"
    config: |
      input:
        generate:
          mapping: '{"test":"value"}'
          interval: "1s"
      output:
        drop: {}
    enabled: true
`
	if err := ioutil.WriteFile(configPath, []byte(sampleConfig), 0644); err != nil {
		t.Fatalf("Failed to write test config file: %v", err)
	}

	// Create a test logger
	logger := log.New(os.Stdout, "TEST: ", log.LstdFlags)

	// Create a new agent
	agent, err := NewAgent(configPath, logger, configDir)
	if err != nil {
		t.Fatalf("Failed to create agent: %v", err)
	}

	// Override ticker interval for faster testing
	agent.TickerInterval = 10 * time.Millisecond

	// Replace the BenthosManager with our mock implementation
	agent.BenthosManager = &mockBenthosManager{configDir: configDir}

	// Start the agent
	if err := agent.Start(); err != nil {
		t.Fatalf("Failed to start agent: %v", err)
	}

	// Wait for initial state transitions (the agent should load config and start instances)
	time.Sleep(100 * time.Millisecond)

	// Check initial state
	agent.mu.RLock()
	instance, exists := agent.Instances["test-instance"]
	agent.mu.RUnlock()

	if !exists {
		t.Fatalf("Test instance not found")
	}

	// Verify the instance is in Running state
	if instance.GetState() != models.StateRunning {
		t.Errorf("Expected instance to be in StateRunning, got %s", instance.GetState())
	}

	// Send a stop event
	agent.Events <- models.NewEvent("test-instance", models.EventStop, nil)

	// Wait for state transition
	time.Sleep(100 * time.Millisecond)

	// Verify the instance is in Stopped state
	if instance.GetState() != models.StateStopped {
		t.Errorf("Expected instance to be in StateStopped, got %s", instance.GetState())
	}

	// Send a start event
	agent.Events <- models.NewEvent("test-instance", models.EventStart, nil)

	// Wait for state transition
	time.Sleep(100 * time.Millisecond)

	// Verify the instance is in Running state again
	if instance.GetState() != models.StateRunning {
		t.Errorf("Expected instance to be in StateRunning, got %s", instance.GetState())
	}

	// Send a configuration update event
	newConfig := `
input:
  generate:
    mapping: '{"updated":"config"}'
    interval: "2s"
output:
  drop: {}
`
	agent.Events <- models.NewEvent("test-instance", models.EventUpdateConfig, newConfig)

	// Wait for state transitions
	time.Sleep(100 * time.Millisecond)

	// Verify the instance went through config change states and is running again
	if instance.GetState() != models.StateRunning {
		t.Errorf("Expected instance to be back in StateRunning after config update, got %s", instance.GetState())
	}

	// Verify configuration was updated
	agent.mu.RLock()
	if instance.CurrentConfig == nil || instance.CurrentConfig.Content != newConfig {
		t.Errorf("Configuration was not updated correctly")
	}
	agent.mu.RUnlock()

	// Stop the agent
	agent.Stop()
}

// mockBenthosManager is a mock implementation of the BenthosManager interface for testing
type mockBenthosManager struct {
	configDir string
	running   map[string]bool
}

// Ensure mockBenthosManager implements benthos.BenthosManager
var _ benthos.BenthosManager = (*mockBenthosManager)(nil)

// WriteConfig writes a Benthos configuration to disk
func (m *mockBenthosManager) WriteConfig(id, content string) (string, error) {
	configPath := filepath.Join(m.configDir, id+".yaml")
	if err := ioutil.WriteFile(configPath, []byte(content), 0644); err != nil {
		return "", err
	}
	return configPath, nil
}

// Start simulates starting a Benthos process
func (m *mockBenthosManager) Start(ctx context.Context, id, configPath string) error {
	if m.running == nil {
		m.running = make(map[string]bool)
	}
	m.running[id] = true
	return nil
}

// Stop simulates stopping a Benthos process
func (m *mockBenthosManager) Stop(ctx context.Context, id string) error {
	if m.running == nil {
		m.running = make(map[string]bool)
	}
	delete(m.running, id)
	return nil
}

// Update simulates updating a Benthos process with new configuration
func (m *mockBenthosManager) Update(ctx context.Context, id, content string) error {
	configPath, err := m.WriteConfig(id, content)
	if err != nil {
		return err
	}

	// Simulate stopping then starting
	m.Stop(ctx, id)
	return m.Start(ctx, id, configPath)
}

// IsRunning checks if a Benthos process is running
func (m *mockBenthosManager) IsRunning(id string) bool {
	if m.running == nil {
		m.running = make(map[string]bool)
	}
	return m.running[id]
}

// GetAllRunningProcesses returns a list of all running process IDs
func (m *mockBenthosManager) GetAllRunningProcesses() []string {
	if m.running == nil {
		m.running = make(map[string]bool)
	}

	var result []string
	for id, running := range m.running {
		if running {
			result = append(result, id)
		}
	}
	return result
}
