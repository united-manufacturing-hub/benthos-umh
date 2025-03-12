package benthosfsm_test

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/looplab/fsm"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/benthosfsm"
)

func TestBenthosfsm(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Benthosfsm Suite")
}

// MockServiceManager is a mock implementation of ServiceManager for testing
type MockServiceManager struct {
	services       map[string]benthosfsm.S6Service
	configurations map[string]string
	startErrors    map[string]error
	stopErrors     map[string]error
	updateErrors   map[string]error
	configError    error
}

func NewMockServiceManager() *MockServiceManager {
	return &MockServiceManager{
		services:       make(map[string]benthosfsm.S6Service),
		configurations: make(map[string]string),
		startErrors:    make(map[string]error),
		stopErrors:     make(map[string]error),
		updateErrors:   make(map[string]error),
	}
}

func (m *MockServiceManager) RegisterService(id string, service benthosfsm.S6Service) {
	m.services[id] = service
}

func (m *MockServiceManager) GetService(id string) (benthosfsm.S6Service, bool) {
	service, exists := m.services[id]
	return service, exists
}

func (m *MockServiceManager) Start(ctx context.Context, id string) error {
	if err, exists := m.startErrors[id]; exists && err != nil {
		return err
	}

	service, exists := m.services[id]
	if !exists {
		return errors.New("service not found")
	}

	return service.Start(ctx)
}

func (m *MockServiceManager) Stop(ctx context.Context, id string) error {
	if err, exists := m.stopErrors[id]; exists && err != nil {
		return err
	}

	service, exists := m.services[id]
	if !exists {
		return errors.New("service not found")
	}

	return service.Stop(ctx)
}

func (m *MockServiceManager) Update(ctx context.Context, id string, content string) error {
	if err, exists := m.updateErrors[id]; exists && err != nil {
		return err
	}

	m.configurations[id] = content
	return nil
}

func (m *MockServiceManager) WriteConfig(id string, content string) (string, error) {
	m.configurations[id] = content
	return "/tmp/config_" + id + ".yaml", nil
}

func (m *MockServiceManager) IsRunning(id string) bool {
	service, exists := m.services[id]
	if !exists {
		return false
	}
	return service.IsRunning()
}

// Set a configuration-related error for a service
func (m *MockServiceManager) SetUpdateError(id string, err error) {
	m.updateErrors[id] = err
}

// Set a start error that will be returned when attempting to start a service
func (m *MockServiceManager) SetStartError(id string, err error) {
	m.startErrors[id] = err
}

// Clear all errors for a specific service
func (m *MockServiceManager) ClearErrors(id string) {
	delete(m.startErrors, id)
	delete(m.stopErrors, id)
	delete(m.updateErrors, id)
}

// Set a configuration error for the service manager
func (m *MockServiceManager) SetConfigError(err error) {
	m.configError = err
}

// Clear the configuration error for the service manager
func (m *MockServiceManager) ClearConfigError() {
	m.configError = nil
}

// MockS6Service is a mock implementation of S6Service for testing
type MockS6Service struct {
	name           string
	status         benthosfsm.S6ServiceStatus
	running        bool
	startCallCount int
	stopCallCount  int
	startError     error
	stopError      error
}

func NewMockS6Service(name string) *MockS6Service {
	return &MockS6Service{
		name:   name,
		status: benthosfsm.S6ServiceDown,
	}
}

func (s *MockS6Service) Start(ctx context.Context) error {
	s.startCallCount++
	if s.startError != nil {
		return s.startError
	}
	s.running = true
	s.status = benthosfsm.S6ServiceUp
	return nil
}

func (s *MockS6Service) Stop(ctx context.Context) error {
	s.stopCallCount++
	if s.stopError != nil {
		return s.stopError
	}
	s.running = false
	s.status = benthosfsm.S6ServiceDown
	return nil
}

func (s *MockS6Service) IsRunning() bool {
	return s.running
}

func (s *MockS6Service) GetStatus() benthosfsm.S6ServiceStatus {
	return s.status
}

var _ = Describe("BenthosInstance FSM", func() {
	var (
		instance       *benthosfsm.BenthosInstance
		serviceManager *MockServiceManager
		s6Service      *MockS6Service
		ctx            context.Context
		callbackCalled map[string]bool
	)

	BeforeEach(func() {
		ctx = context.Background()
		callbackCalled = make(map[string]bool)

		callbacks := make(map[string]fsm.Callback)
		callbacks["enter_"+benthosfsm.StateStopped] = func(ctx context.Context, e *fsm.Event) {
			callbackCalled["enter_"+benthosfsm.StateStopped] = true
		}
		callbacks["enter_"+benthosfsm.StateStarting] = func(ctx context.Context, e *fsm.Event) {
			callbackCalled["enter_"+benthosfsm.StateStarting] = true
		}
		callbacks["enter_"+benthosfsm.StateRunning] = func(ctx context.Context, e *fsm.Event) {
			callbackCalled["enter_"+benthosfsm.StateRunning] = true
		}

		instance = benthosfsm.NewBenthosInstance("test", callbacks)
		serviceManager = NewMockServiceManager()
		instance.Process = serviceManager

		// Set initial configuration
		initialConfig := &benthosfsm.BenthosConfig{
			ConfigPath: "test.yaml",
			Content:    "test-content",
			Version:    "1",
		}
		instance.SetCurrentConfig(initialConfig)
		instance.AddToConfigHistory(initialConfig, benthosfsm.ConfigStatusSuccess)

		// Create a mock S6 service
		s6Service = NewMockS6Service("test-service")
		serviceManager.RegisterService("test", s6Service)
	})

	Describe("Initialization", func() {
		It("should start in the stopped state", func() {
			Expect(instance.GetState()).To(Equal(benthosfsm.StateStopped))
		})

		It("should have desired state set to stopped", func() {
			Expect(instance.GetDesiredState()).To(Equal(benthosfsm.StateStopped))
		})
	})

	Describe("State transitions", func() {
		It("should transition from stopped to starting to running", func() {
			// Set desired state to running
			instance.SetDesiredState(benthosfsm.StateRunning)
			Expect(instance.GetDesiredState()).To(Equal(benthosfsm.StateRunning))

			// Reconcile to start
			err := benthosfsm.ReconcileInstance(ctx, instance, serviceManager)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateStarting))
			Expect(callbackCalled["enter_"+benthosfsm.StateStarting]).To(BeTrue())

			// Start the service
			err = benthosfsm.StartBenthos(ctx, instance, serviceManager)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateRunning))
			Expect(callbackCalled["enter_"+benthosfsm.StateRunning]).To(BeTrue())
			Expect(s6Service.startCallCount).To(Equal(1))
			Expect(s6Service.IsRunning()).To(BeTrue())
		})

		It("should transition from running to stopped", func() {
			// First get to running state
			instance.SetDesiredState(benthosfsm.StateRunning)
			err := benthosfsm.ReconcileInstance(ctx, instance, serviceManager)
			Expect(err).NotTo(HaveOccurred())
			err = benthosfsm.StartBenthos(ctx, instance, serviceManager)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateRunning))

			// Now set desired state to stopped
			instance.SetDesiredState(benthosfsm.StateStopped)
			err = benthosfsm.ReconcileInstance(ctx, instance, serviceManager)
			Expect(err).NotTo(HaveOccurred())
			err = benthosfsm.StopBenthos(ctx, instance, serviceManager)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateStopped))
			Expect(callbackCalled["enter_"+benthosfsm.StateStopped]).To(BeTrue())
			Expect(s6Service.stopCallCount).To(Equal(1))
			Expect(s6Service.IsRunning()).To(BeFalse())
		})

		FIt("should handle configuration changes", func() {
			// Start the instance
			instance.SetDesiredState(benthosfsm.StateRunning)
			err := instance.SendEvent(ctx, benthosfsm.EventStart)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateStarting))

			// Transition to running
			err = instance.SendEvent(ctx, benthosfsm.EventStarted)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateRunning))

			// Update configuration
			newConfig := benthosfsm.NewBenthosConfig("new config")
			configPath, err := serviceManager.WriteConfig("test", newConfig.Content)
			Expect(err).NotTo(HaveOccurred())
			newConfig.SetConfigPath(configPath)

			// Set the desired config
			instance.SetDesiredConfig(newConfig)
			Expect(instance.GetDesiredConfig()).To(Equal(newConfig))

			// Trigger config change
			err = benthosfsm.ReconcileInstance(ctx, instance, serviceManager)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateConfigChanged))

			// Move to changing config state
			err = benthosfsm.ReconcileInstance(ctx, instance, serviceManager)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateChangingConfig))

			// Apply the config change
			err = benthosfsm.ReconcileInstance(ctx, instance, serviceManager)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateStarting))
			Expect(instance.GetCurrentConfig()).To(Equal(newConfig))
			Expect(instance.GetDesiredConfig()).To(BeNil())

			// Final transition back to running
			err = benthosfsm.ReconcileInstance(ctx, instance, serviceManager)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateRunning))
		})
	})

	Describe("Error handling", func() {
		It("should transition to error state on service start failure", func() {
			// Set the service to fail on start
			s6Service.startError = errors.New("start failed")

			// Set desired state to running
			instance.SetDesiredState(benthosfsm.StateRunning)
			err := instance.SendEvent(ctx, benthosfsm.EventStart)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateStarting))

			// Try to start the service
			err = benthosfsm.StartBenthos(ctx, instance, serviceManager)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("start failed"))

			// Send fail event
			err = instance.SendEvent(ctx, benthosfsm.EventFail, err)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateError))
			Expect(instance.GetError()).NotTo(BeNil())
			Expect(instance.GetError().Error()).To(ContainSubstring("start failed"))
		})

		It("should retry after an error with backoff", func() {
			// Set the service to fail on start
			s6Service.startError = errors.New("start failed")

			// Set desired state to running
			instance.SetDesiredState(benthosfsm.StateRunning)
			err := instance.SendEvent(ctx, benthosfsm.EventStart)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateStarting))

			// Try to start the service (will fail)
			err = benthosfsm.StartBenthos(ctx, instance, serviceManager)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("start failed"))

			// Send fail event
			err = instance.SendEvent(ctx, benthosfsm.EventFail, err)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateError))
			Expect(instance.GetError()).NotTo(BeNil())

			// Should not retry immediately
			err = benthosfsm.HandleErrorState(ctx, instance)
			Expect(err).To(BeNil())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateError))

			// Advance time to simulate backoff expiry
			instance.SetNextRetryTime(time.Now().Add(-1 * time.Second))
			Expect(instance.ShouldRetry()).To(BeTrue())

			// Clear the error to allow retry to succeed
			s6Service.startError = nil

			// Now it should retry
			err = benthosfsm.HandleErrorState(ctx, instance)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthosfsm.StateStopped))
			Expect(instance.GetError()).To(BeNil())
		})
	})
})

var _ = Describe("Configuration Rollback", func() {
	var (
		instance       *benthosfsm.BenthosInstance
		serviceManager *MockServiceManager
		s6Service      *MockS6Service
		ctx            context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()

		// Create a new instance
		instance = benthosfsm.NewBenthosInstance("test", nil)
		Expect(instance).NotTo(BeNil())

		// Create a new service manager
		s6Service = NewMockS6Service("test-service")
		serviceManager = NewMockServiceManager()
		serviceManager.RegisterService("test", s6Service)
		Expect(serviceManager).NotTo(BeNil())

		// Register callbacks
		benthosfsm.RegisterCallbacks(instance, serviceManager)

		// Create initial config
		initialConfig := benthosfsm.NewBenthosConfig("initial config")
		configPath, err := serviceManager.WriteConfig("test", initialConfig.Content)
		Expect(err).NotTo(HaveOccurred())
		initialConfig.SetConfigPath(configPath)

		// Set the current config
		instance.SetCurrentConfig(initialConfig)
		Expect(instance.GetCurrentConfig()).To(Equal(initialConfig))

		// Start the instance
		instance.SetDesiredState(benthosfsm.StateRunning)
		err = instance.SendEvent(ctx, benthosfsm.EventStart)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthosfsm.StateStarting))

		// Add initial config to history
		instance.AddToConfigHistory(initialConfig, benthosfsm.ConfigStatusSuccess)

		// Log success
		log.Printf("Instance test successfully running with config version %s", initialConfig.Version)
	})

	AfterEach(func() {
		serviceManager.ClearErrors("test")
	})

	It("should add configurations to history when successfully applied", func() {
		// Verify initial config was added to history
		history := instance.GetConfigHistory()
		Expect(history).To(HaveLen(1))
		Expect(history[0].Status).To(Equal(benthosfsm.ConfigStatusSuccess))
		Expect(history[0].Config.Content).To(Equal("initial config"))

		// Apply a new configuration
		newConfig := benthosfsm.NewBenthosConfig("new config")
		configPath, _ := serviceManager.WriteConfig("test", newConfig.Content)
		newConfig.SetConfigPath(configPath)
		instance.SetDesiredConfig(newConfig)

		// Trigger config change flow
		err := instance.SendEvent(ctx, benthosfsm.EventConfigChange)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthosfsm.StateConfigChanged))

		err = instance.SendEvent(ctx, benthosfsm.EventUpdateConfig)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthosfsm.StateChangingConfig))

		err = benthosfsm.UpdateBenthosConfig(ctx, instance, serviceManager)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthosfsm.StateStarting))

		err = instance.SendEvent(ctx, benthosfsm.EventStarted)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthosfsm.StateRunning))

		// Verify both configs are in history
		history = instance.GetConfigHistory()
		Expect(history).To(HaveLen(2))
		Expect(history[0].Status).To(Equal(benthosfsm.ConfigStatusSuccess))
		Expect(history[0].Config.Content).To(Equal("new config"))
		Expect(history[1].Status).To(Equal(benthosfsm.ConfigStatusSuccess))
		Expect(history[1].Config.Content).To(Equal("initial config"))
	})

	It("should rollback to previous configuration when update fails", func() {
		// Set up a config update error
		serviceManager.SetUpdateError("test", errors.New("config validation error: invalid yaml"))

		// Create a new (invalid) configuration
		badConfig := benthosfsm.NewBenthosConfig("bad: : config: content: :")
		configPath, _ := serviceManager.WriteConfig("test", badConfig.Content)
		badConfig.SetConfigPath(configPath)
		instance.SetDesiredConfig(badConfig)

		// Trigger config change flow
		err := instance.SendEvent(ctx, benthosfsm.EventConfigChange)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthosfsm.StateConfigChanged))

		err = instance.SendEvent(ctx, benthosfsm.EventUpdateConfig)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthosfsm.StateChangingConfig))

		// Attempt to update - should fail and enter config error state
		err = benthosfsm.UpdateBenthosConfig(ctx, instance, serviceManager)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthosfsm.StateConfigError))
		Expect(instance.GetConfigError()).NotTo(BeNil())

		// Verify the failed config was added to history
		history := instance.GetConfigHistory()
		var foundBadConfig bool
		for _, entry := range history {
			if entry.Status == benthosfsm.ConfigStatusError && entry.Config.Content == badConfig.Content {
				foundBadConfig = true
				break
			}
		}
		Expect(foundBadConfig).To(BeTrue())

		// Now initiate rollback
		err = benthosfsm.HandleConfigErrorState(ctx, instance)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthosfsm.StateChangingConfig))

		// Clear the error to allow the rollback to succeed
		serviceManager.ClearErrors("test")

		// Complete the rollback
		err = benthosfsm.RollbackBenthosConfig(ctx, instance, serviceManager)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthosfsm.StateStarting))

		// Complete the startup
		err = instance.SendEvent(ctx, benthosfsm.EventStarted)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthosfsm.StateRunning))

		// Verify the current config is the original one
		currentConfig := instance.GetCurrentConfig()
		Expect(currentConfig).NotTo(BeNil())
		Expect(currentConfig.Content).To(Equal("initial config"))
	})

	It("should detect config errors during startup and rollback", func() {
		// Apply a successful configuration first to have history
		successConfig := benthosfsm.NewBenthosConfig("successful config")
		configPath, _ := serviceManager.WriteConfig("test", successConfig.Content)
		successConfig.SetConfigPath(configPath)
		instance.SetCurrentConfig(successConfig)

		// Add to history as a successful config
		instance.AddToConfigHistory(successConfig, benthosfsm.ConfigStatusSuccess)

		// Set up a bad config that will fail during startup
		badConfig := benthosfsm.NewBenthosConfig("bad config that will fail on startup")
		configPath, _ = serviceManager.WriteConfig("test", badConfig.Content)
		badConfig.SetConfigPath(configPath)
		instance.SetCurrentConfig(badConfig)

		// Set the service to fail on startup with a config error
		serviceManager.SetStartError("test", errors.New("config parsing error: invalid configuration"))

		// Start should fail with a config error
		err := benthosfsm.StartBenthos(ctx, instance, serviceManager)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthosfsm.StateConfigError))

		// Clear the error so rollback can succeed
		serviceManager.ClearErrors("test")

		// Trigger the rollback
		err = benthosfsm.HandleConfigErrorState(ctx, instance)
		Expect(err).NotTo(HaveOccurred())

		// Complete the rollback
		err = benthosfsm.RollbackBenthosConfig(ctx, instance, serviceManager)
		Expect(err).NotTo(HaveOccurred())

		// Complete the startup
		err = instance.SendEvent(ctx, benthosfsm.EventStarted)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthosfsm.StateRunning))

		// Verify we rolled back to the successful config
		currentConfig := instance.GetCurrentConfig()
		Expect(currentConfig).NotTo(BeNil())
		Expect(currentConfig.Content).To(Equal("successful config"))
	})

	It("should maintain a history of configurations with proper statuses", func() {
		// Create several configurations and apply them
		configs := []string{
			"config version 1",
			"config version 2",
			"config version 3 - will fail",
			"config version 4",
			"config version 5",
		}

		// Apply the first two configurations successfully
		for i := 0; i < 2; i++ {
			config := benthosfsm.NewBenthosConfig(configs[i])
			configPath, _ := serviceManager.WriteConfig("test", config.Content)
			config.SetConfigPath(configPath)

			instance.SetDesiredConfig(config)
			err := instance.SendEvent(ctx, benthosfsm.EventConfigChange)
			Expect(err).NotTo(HaveOccurred())

			err = instance.SendEvent(ctx, benthosfsm.EventUpdateConfig)
			Expect(err).NotTo(HaveOccurred())

			err = benthosfsm.UpdateBenthosConfig(ctx, instance, serviceManager)
			Expect(err).NotTo(HaveOccurred())

			err = instance.SendEvent(ctx, benthosfsm.EventStarted)
			Expect(err).NotTo(HaveOccurred())
		}

		// The third configuration will fail
		serviceManager.SetUpdateError("test", errors.New("config validation error"))

		badConfig := benthosfsm.NewBenthosConfig(configs[2])
		configPath, _ := serviceManager.WriteConfig("test", badConfig.Content)
		badConfig.SetConfigPath(configPath)

		instance.SetDesiredConfig(badConfig)
		err := instance.SendEvent(ctx, benthosfsm.EventConfigChange)
		Expect(err).NotTo(HaveOccurred())

		err = instance.SendEvent(ctx, benthosfsm.EventUpdateConfig)
		Expect(err).NotTo(HaveOccurred())

		// This should fail and add to history with error status
		err = benthosfsm.UpdateBenthosConfig(ctx, instance, serviceManager)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthosfsm.StateConfigError))

		// Clear error and rollback
		serviceManager.ClearErrors("test")
		err = benthosfsm.HandleConfigErrorState(ctx, instance)
		Expect(err).NotTo(HaveOccurred())

		err = benthosfsm.RollbackBenthosConfig(ctx, instance, serviceManager)
		Expect(err).NotTo(HaveOccurred())

		err = instance.SendEvent(ctx, benthosfsm.EventStarted)
		Expect(err).NotTo(HaveOccurred())

		// Apply two more successful configurations
		for i := 3; i < 5; i++ {
			config := benthosfsm.NewBenthosConfig(configs[i])
			configPath, _ := serviceManager.WriteConfig("test", config.Content)
			config.SetConfigPath(configPath)

			instance.SetDesiredConfig(config)
			err := instance.SendEvent(ctx, benthosfsm.EventConfigChange)
			Expect(err).NotTo(HaveOccurred())

			err = instance.SendEvent(ctx, benthosfsm.EventUpdateConfig)
			Expect(err).NotTo(HaveOccurred())

			err = benthosfsm.UpdateBenthosConfig(ctx, instance, serviceManager)
			Expect(err).NotTo(HaveOccurred())

			err = instance.SendEvent(ctx, benthosfsm.EventStarted)
			Expect(err).NotTo(HaveOccurred())
		}

		// Check the history - should contain all five configurations with correct statuses
		history := instance.GetConfigHistory()

		// There should be at least 6 entries: initial + 5 from this test
		Expect(len(history)).To(BeNumerically(">=", 6))

		// The most recent should be config version 5
		Expect(history[0].Config.Content).To(Equal("config version 5"))
		Expect(history[0].Status).To(Equal(benthosfsm.ConfigStatusSuccess))

		// Config version 3 should have error status
		var foundErrorConfig bool
		for _, entry := range history {
			if entry.Config.Content == "config version 3 - will fail" {
				Expect(entry.Status).To(Equal(benthosfsm.ConfigStatusError))
				foundErrorConfig = true
				break
			}
		}
		Expect(foundErrorConfig).To(BeTrue())
	})
})
