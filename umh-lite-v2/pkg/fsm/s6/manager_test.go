package s6

import (
	"context"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/pkg/config"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/pkg/service/s6"
)

// createMockS6Instance creates an S6Instance with a mock service for testing
func createMockS6Instance(manager *S6Manager, serviceName string, mockService s6service.Service, desiredState string) *S6Instance {
	cfg := config.S6FSMConfig{
		Name:         serviceName,
		DesiredState: desiredState,
		S6ServiceConfig: s6service.S6ServiceConfig{
			Command:     []string{"/bin/sh", "-c", "echo test"},
			Env:         map[string]string{},
			ConfigFiles: map[string]string{},
		},
	}

	// Create an instance with the mock service
	instance := NewS6Instance(serviceName, cfg)
	instance.service = mockService
	instance.servicePath = filepath.Join("/mock/path", serviceName)

	// Add it to the manager
	manager.Instances[serviceName] = instance

	return instance
}

// setS6InstanceState sets the state of an S6Instance for testing
func setS6InstanceState(instance *S6Instance, state string) {
	instance.baseFSMInstance.SetCurrentFSMState(state)
}

// configureServiceForState sets up the mock service to match the expected state
func configureServiceForState(mockService *s6service.MockService, servicePath string, state string) {
	mockService.ExistingServices[servicePath] = true

	// Configure service state based on FSM state
	switch state {
	case OperationalStateRunning:
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceUp,
			Uptime: 10,
			Pid:    12345,
		}
	case OperationalStateStopped:
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}
	case OperationalStateStarting:
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceRestarting, // Use as proxy for "starting"
		}
	case OperationalStateStopping:
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown, // Use as proxy for "stopping"
		}
	default:
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceUnknown,
		}
	}
}

var _ = FDescribe("S6Manager", func() {
	var (
		manager *S6Manager
		ctx     context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		manager = NewS6Manager()
	})

	It("should handle empty config without errors", func() {
		// Create empty config
		emptyConfig := config.FullConfig{}

		// Reconcile with empty config
		err := manager.Reconcile(ctx, emptyConfig)
		Expect(err).NotTo(HaveOccurred())

		// Verify that no instances were created
		Expect(manager.Instances).To(BeEmpty())
	})

	It("should add a service when it appears in config", func() {
		// Start with empty config
		emptyConfig := config.FullConfig{}
		err := manager.Reconcile(ctx, emptyConfig)
		Expect(err).NotTo(HaveOccurred())
		Expect(manager.Instances).To(BeEmpty())

		// Create config with one service
		configWithService := config.FullConfig{
			Services: []config.S6FSMConfig{
				{
					Name:         "test-service",
					DesiredState: OperationalStateRunning,
					S6ServiceConfig: s6service.S6ServiceConfig{
						Command:     []string{"/bin/sh", "-c", "echo hello"},
						Env:         map[string]string{"TEST_ENV": "test_value"},
						ConfigFiles: map[string]string{},
					},
				},
			},
		}

		// Reconcile with the new config
		err = manager.Reconcile(ctx, configWithService)
		Expect(err).NotTo(HaveOccurred())

		// Verify service was created
		Expect(manager.Instances).To(HaveLen(1))
		Expect(manager.Instances).To(HaveKey("test-service"))

		// Verify service properties
		service := manager.Instances["test-service"]
		Expect(service.GetDesiredFSMState()).To(Equal(OperationalStateRunning))
		Expect(service.config.Name).To(Equal("test-service"))
	})

	It("should go through proper state transitions when starting a service", func() {
		// Create a mock service
		mockService := s6service.NewMockService()
		serviceName := "test-transition"
		servicePath := filepath.Join("/mock/path", serviceName)

		// Create a mock service instance and manually add to manager
		createMockS6Instance(manager, serviceName, mockService, OperationalStateRunning)

		// Verify initial state
		Expect(manager.Instances).To(HaveLen(1))
		Expect(manager.Instances).To(HaveKey(serviceName))

		// Reset the state to to_be_created to simulate a fresh instance
		manager.Instances[serviceName].baseFSMInstance.SetCurrentFSMState(internal_fsm.LifecycleStateToBeCreated)
		Expect(manager.Instances[serviceName].GetCurrentFSMState()).To(Equal(internal_fsm.LifecycleStateToBeCreated))

		// Create a config that includes our service to ensure it doesn't get removed
		configWithService := config.FullConfig{
			Services: []config.S6FSMConfig{
				{
					Name:         serviceName,
					DesiredState: OperationalStateRunning,
					S6ServiceConfig: s6service.S6ServiceConfig{
						Command:     []string{"/bin/sh", "-c", "echo test"},
						Env:         map[string]string{},
						ConfigFiles: map[string]string{},
					},
				},
			},
		}

		// First reconcile - should transition to creating
		err := manager.Reconcile(ctx, configWithService)
		Expect(err).NotTo(HaveOccurred())
		Expect(manager.Instances[serviceName].GetCurrentFSMState()).To(Equal(internal_fsm.LifecycleStateCreating))

		// Mock service as created and down
		mockService.ExistingServices[servicePath] = true
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}

		// Second reconcile - should transition to stopped state
		err = manager.Reconcile(ctx, configWithService)
		Expect(err).NotTo(HaveOccurred())
		Expect(manager.Instances[serviceName].GetCurrentFSMState()).To(Equal(OperationalStateStopped))

		// Third reconcile - should transition to starting
		err = manager.Reconcile(ctx, configWithService)
		Expect(err).NotTo(HaveOccurred())
		Expect(manager.Instances[serviceName].GetCurrentFSMState()).To(Equal(OperationalStateStarting))

		// Mock service as running
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceUp,
			Uptime: 5,
			Pid:    12345,
		}

		// Fourth reconcile - should transition to running
		err = manager.Reconcile(ctx, configWithService)
		Expect(err).NotTo(HaveOccurred())
		Expect(manager.Instances[serviceName].GetCurrentFSMState()).To(Equal(OperationalStateRunning))

		// Verify the service has desired state running and is actually running
		service := manager.Instances[serviceName]
		Expect(service.GetDesiredFSMState()).To(Equal(OperationalStateRunning))
		Expect(service.ObservedState.ServiceInfo.Status).To(Equal(s6service.ServiceUp))
		Expect(service.ObservedState.ServiceInfo.Pid).To(Equal(12345))
	})

	FIt("should demonstrate full service lifecycle from creation to removal", func() {
		// Create a mock service for testing
		mockService := s6service.NewMockService()
		serviceName := "test-lifecycle"

		// 1. Start with empty config to verify clean state
		emptyConfig := config.FullConfig{}
		err := manager.Reconcile(ctx, emptyConfig)
		Expect(err).NotTo(HaveOccurred())
		Expect(manager.Instances).To(BeEmpty(), "Should start with no services")

		// 2. Create config with one service to trigger creation and startup
		GinkgoWriter.Printf("\n=== PHASE 1: CREATING SERVICE ===\n")
		configWithService := config.FullConfig{
			Services: []config.S6FSMConfig{
				{
					Name:         serviceName,
					DesiredState: OperationalStateRunning,
					S6ServiceConfig: s6service.S6ServiceConfig{
						Command:     []string{"/bin/sh", "-c", "echo test-service"},
						Env:         map[string]string{"TEST": "value"},
						ConfigFiles: map[string]string{},
					},
				},
			},
		}

		// Reconcile once to let the instance be created
		GinkgoWriter.Printf("Reconciling once to set up mock service\n")
		err = manager.Reconcile(ctx, configWithService)
		Expect(err).NotTo(HaveOccurred())
		Expect(manager.Instances).To(HaveKey(serviceName), "Service should be created")

		// Get the instance and replace its service with our mock
		instance := manager.Instances[serviceName]
		GinkgoWriter.Printf("Service path is: %s\n", instance.servicePath)

		// Make sure we're using the correct path in our mock
		servicePath := instance.servicePath

		// Set up the mock service properly
		mockService.ExistingServices[servicePath] = true
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}
		mockService.GetConfigResult = s6service.S6ServiceConfig{
			Command: []string{"/bin/sh", "-c", "echo test-service"},
			Env:     map[string]string{"TEST": "value"},
		}

		// Replace the real service with our mock
		instance.service = mockService

		GinkgoWriter.Printf("Mock service properly configured. ExistingServices: %v\n", mockService.ExistingServices)

		// Loop to bring service to running state
		maxCreationAttempts := 10
		serviceRunning := false

		for i := 1; i <= maxCreationAttempts; i++ {
			// Reconcile with the service config
			GinkgoWriter.Printf("\n--- Creation reconcile #%d ---\n", i)
			err = manager.Reconcile(ctx, configWithService)
			Expect(err).NotTo(HaveOccurred())

			// Log current state after reconciliation
			currentState := instance.GetCurrentFSMState()
			desiredState := instance.GetDesiredFSMState()

			// Get service status and other info
			serviceStatus := "unknown"
			if info, exists := mockService.ServiceStates[servicePath]; exists {
				serviceStatus = string(info.Status)
			}

			GinkgoWriter.Printf("Current state: %s\n", currentState)
			GinkgoWriter.Printf("Desired state: %s\n", desiredState)
			GinkgoWriter.Printf("Service status: %s\n", serviceStatus)
			GinkgoWriter.Printf("Service created in mock: %t\n", mockService.ExistingServices[servicePath])

			// Print the error state if any
			if instance.baseFSMInstance.GetError() != nil {
				GinkgoWriter.Printf("FSM Error: %v\n", instance.baseFSMInstance.GetError())
			}

			// Check if service reached running state
			if currentState == OperationalStateRunning {
				serviceRunning = true
				GinkgoWriter.Printf("Service reached RUNNING state after %d iterations\n", i)
				break
			}
		}

		// Verify the service reached running state
		Expect(serviceRunning).To(BeTrue(), "Service should have reached running state")
		Expect(manager.Instances).To(HaveKey(serviceName))
		Expect(manager.Instances[serviceName].GetCurrentFSMState()).To(Equal(OperationalStateRunning))

		// 3. Now remove the service by using empty config again
		GinkgoWriter.Printf("\n=== PHASE 2: REMOVING SERVICE ===\n")

		// Get reference to instance before removal
		instance = manager.Instances[serviceName]
		currentState := instance.GetCurrentFSMState()
		desiredState := instance.GetDesiredFSMState()
		serviceStatus := string(mockService.ServiceStates[servicePath].Status)

		GinkgoWriter.Printf("Before removal: Current=%s, Desired=%s, ServiceStatus=%s\n",
			currentState, desiredState, serviceStatus)

		// Loop to remove service
		maxRemovalAttempts := 15
		serviceRemoved := false

		// First ensure we're in the required state for removal
		if currentState != OperationalStateStopped {
			GinkgoWriter.Printf("Service is not in stopped state, setting desired state to stopped\n")
			instance.SetDesiredFSMState(OperationalStateStopped)

			// Extra reconcile to update desired state
			err = manager.Reconcile(ctx, emptyConfig)
			GinkgoWriter.Printf("After setting desired=stopped: Current=%s, Desired=%s\n",
				instance.GetCurrentFSMState(), instance.GetDesiredFSMState())
		}

		for i := 1; i <= maxRemovalAttempts; i++ {
			// Reconcile with empty config
			GinkgoWriter.Printf("\n--- Removal reconcile #%d ---\n", i)
			err = manager.Reconcile(ctx, emptyConfig)
			Expect(err).NotTo(HaveOccurred())

			// Log state after reconciliation
			if instance, exists := manager.Instances[serviceName]; exists {
				currentState := instance.GetCurrentFSMState()
				desiredState := instance.GetDesiredFSMState()

				serviceStatus := "unknown"
				if info, exists := mockService.ServiceStates[servicePath]; exists {
					serviceStatus = string(info.Status)
				}

				GinkgoWriter.Printf("Current state: %s\n", currentState)
				GinkgoWriter.Printf("Desired state: %s\n", desiredState)
				GinkgoWriter.Printf("Service status: %s\n", serviceStatus)

				// Print the error state if any
				if instance.baseFSMInstance.GetError() != nil {
					GinkgoWriter.Printf("FSM Error: %v\n", instance.baseFSMInstance.GetError())
				}

				// Check for removal
				if currentState == internal_fsm.LifecycleStateRemoved {
					GinkgoWriter.Printf("Service is in REMOVED state, should be deleted soon\n")
				}
			} else {
				GinkgoWriter.Printf("Removal #%d: Service has been removed\n", i)
				serviceRemoved = true
				break
			}
		}

		// Verify the service was removed
		Expect(serviceRemoved).To(BeTrue(), "Service should have been removed")
		Expect(manager.Instances).NotTo(HaveKey(serviceName))
	})
})
