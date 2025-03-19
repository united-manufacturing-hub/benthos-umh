package s6

import (
	"context"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/s6"
)

// createMockS6Instance creates an S6Instance with a mock service for testing
func createMockS6Instance(manager *S6Manager, serviceName string, mockService s6service.Service, desiredState string) *S6Instance {
	cfg := config.S6FSMConfig{
		FSMInstanceConfig: config.FSMInstanceConfig{
			Name:            serviceName,
			DesiredFSMState: desiredState,
		},
		S6ServiceConfig: config.S6ServiceConfig{
			Command:     []string{"/bin/sh", "-c", "echo test"},
			Env:         map[string]string{},
			ConfigFiles: map[string]string{},
		},
	}

	// Create an instance with the mock service
	instance, err := NewS6Instance(serviceName, cfg)
	if err != nil {
		return nil
	}
	instance.service = mockService
	instance.servicePath = filepath.Join("/mock/path", serviceName)

	// Add it to the manager
	manager.AddInstanceForTest(serviceName, instance)

	return instance
}

// setS6InstanceState sets the state of an S6Instance for testing
func setS6InstanceState(instance *S6Instance, state string) {
	instance.baseFSMInstance.SetCurrentFSMState(state)
	instance.baseFSMInstance.SetDesiredFSMState(state)

	// Set observed state based on the FSM state
	switch state {
	case OperationalStateRunning:
		instance.ObservedState.ServiceInfo.Status = s6service.ServiceUp
		instance.ObservedState.ServiceInfo.Uptime = 10
		instance.ObservedState.ServiceInfo.Pid = 12345
	case OperationalStateStopped:
		instance.ObservedState.ServiceInfo.Status = s6service.ServiceDown
	case OperationalStateStarting:
		instance.ObservedState.ServiceInfo.Status = s6service.ServiceRestarting // Use as proxy for "starting"
	case OperationalStateStopping:
		instance.ObservedState.ServiceInfo.Status = s6service.ServiceDown // Use as proxy for "stopping"
	default:
		instance.ObservedState.ServiceInfo.Status = s6service.ServiceUnknown
	}
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

var _ = Describe("S6Manager", func() {
	var (
		manager *S6Manager
		ctx     context.Context
		tick    uint64
	)

	BeforeEach(func() {
		ctx = context.Background()
		manager = NewS6Manager("")
		tick = uint64(0)
	})

	It("should handle empty config without errors", func() {
		// Create empty config
		emptyConfig := []config.S6FSMConfig{}

		// Reconcile with empty config
		err, _ := manager.Reconcile(ctx, config.FullConfig{Services: emptyConfig}, 0)
		Expect(err).NotTo(HaveOccurred())

		// Verify that no instances were created
		Expect(manager.GetInstances()).To(BeEmpty())
	})

	It("should add a service when it appears in config", func() {
		// Start with empty config
		emptyConfig := []config.S6FSMConfig{}
		err, _ := manager.Reconcile(ctx, config.FullConfig{Services: emptyConfig}, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		Expect(manager.GetInstances()).To(BeEmpty())

		// Create config with one service
		configWithService := []config.S6FSMConfig{
			{
				FSMInstanceConfig: config.FSMInstanceConfig{
					Name:            "test-service",
					DesiredFSMState: OperationalStateRunning,
				},
				S6ServiceConfig: config.S6ServiceConfig{
					Command:     []string{"/bin/sh", "-c", "echo hello"},
					Env:         map[string]string{"TEST_ENV": "test_value"},
					ConfigFiles: map[string]string{},
				},
			},
		}

		// Reconcile with the new config
		err, _ = manager.Reconcile(ctx, config.FullConfig{Services: configWithService}, tick)
		Expect(err).NotTo(HaveOccurred())

		err, _ = manager.Reconcile(ctx, config.FullConfig{Services: configWithService}, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())

		// Verify service was created
		Expect(manager.GetInstances()).To(HaveLen(1))
		Expect(manager.GetInstances()).To(HaveKey("test-service"))

		// Verify service properties
		service, ok := manager.GetInstance("test-service")
		Expect(ok).To(BeTrue())
		Expect(service.GetDesiredFSMState()).To(Equal(OperationalStateRunning))

		// Verify the instance is an S6Instance
		s6Instance, ok := service.(*S6Instance)
		Expect(ok).To(BeTrue())
		Expect(s6Instance.config.Name).To(Equal("test-service"))
	})

	It("should go through proper state transitions when starting a service", func() {
		// Create a mock service
		mockService := s6service.NewMockService()
		serviceName := "test-transition"
		servicePath := filepath.Join("/mock/path", serviceName)

		// Create a mock service instance and manually add to manager
		createMockS6Instance(manager, serviceName, mockService, OperationalStateRunning)

		// Verify initial state
		Expect(manager.GetInstances()).To(HaveLen(1))
		Expect(manager.GetInstances()).To(HaveKey(serviceName))

		// Reset the state to to_be_created to simulate a fresh instance
		instance, ok := manager.GetInstance(serviceName)
		Expect(ok).To(BeTrue())
		if s6Instance, ok := instance.(*S6Instance); ok {
			s6Instance.baseFSMInstance.SetCurrentFSMState(internal_fsm.LifecycleStateToBeCreated)
		} else {
			Fail("instance is not an S6Instance")
		}
		Expect(instance.GetCurrentFSMState()).To(Equal(internal_fsm.LifecycleStateToBeCreated))

		// Create a config that includes our service to ensure it doesn't get removed
		configWithService := []config.S6FSMConfig{
			{
				FSMInstanceConfig: config.FSMInstanceConfig{
					Name:            serviceName,
					DesiredFSMState: OperationalStateRunning,
				},
				S6ServiceConfig: config.S6ServiceConfig{
					Command:     []string{"/bin/sh", "-c", "echo test"},
					Env:         map[string]string{},
					ConfigFiles: map[string]string{},
				},
			},
		}

		// econcile - should transition to LifecycleStateToBeCreated
		err, _ := manager.Reconcile(ctx, config.FullConfig{Services: configWithService}, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		instance, ok = manager.GetInstance(serviceName)
		Expect(ok).To(BeTrue())
		Expect(instance.GetCurrentFSMState()).To(Equal(internal_fsm.LifecycleStateToBeCreated))

		// reconcile - should transition to LifecycleStateCreating
		err, _ = manager.Reconcile(ctx, config.FullConfig{Services: configWithService}, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		instance, ok = manager.GetInstance(serviceName)
		Expect(ok).To(BeTrue())
		Expect(instance.GetCurrentFSMState()).To(Equal(internal_fsm.LifecycleStateCreating))

		// Mock service as created and down
		mockService.ExistingServices[servicePath] = true
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}
		// Set up the mock service configuration to match what's expected
		mockService.GetConfigResult = config.S6ServiceConfig{
			Command:     []string{"/bin/sh", "-c", "echo test"},
			Env:         map[string]string{},
			ConfigFiles: map[string]string{},
		}

		// reconcile - should still be in creating state
		err, _ = manager.Reconcile(ctx, config.FullConfig{Services: configWithService}, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		instance, ok = manager.GetInstance(serviceName)
		Expect(ok).To(BeTrue())
		Expect(instance.GetCurrentFSMState()).To(Equal(internal_fsm.LifecycleStateCreating))

		// reconcile - should transition to stopped state
		err, _ = manager.Reconcile(ctx, config.FullConfig{Services: configWithService}, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		instance, ok = manager.GetInstance(serviceName)
		Expect(ok).To(BeTrue())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped))

		// reconcile - should transition to starting
		err, _ = manager.Reconcile(ctx, config.FullConfig{Services: configWithService}, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		instance, ok = manager.GetInstance(serviceName)
		Expect(ok).To(BeTrue())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting))

		// Mock service as running
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceUp,
			Uptime: 5,
			Pid:    12345,
		}

		// reconcile - should transition to running
		err, _ = manager.Reconcile(ctx, config.FullConfig{Services: configWithService}, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		instance, ok = manager.GetInstance(serviceName)
		Expect(ok).To(BeTrue())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateRunning))

		// Verify the service has desired state running and is actually running
		if s6Instance, ok := instance.(*S6Instance); ok {
			Expect(s6Instance.GetDesiredFSMState()).To(Equal(OperationalStateRunning))
			Expect(s6Instance.ObservedState.ServiceInfo.Status).To(Equal(s6service.ServiceUp))
			Expect(s6Instance.ObservedState.ServiceInfo.Pid).To(Equal(12345))
		} else {
			Fail("instance is not an S6Instance")
		}
	})

	It("should demonstrate full service lifecycle from creation to removal", func() {
		// Create a mock service for testing
		mockService := s6service.NewMockService()
		serviceName := "test-lifecycle"

		// 1. Start with empty config to verify clean state
		emptyConfig := []config.S6FSMConfig{}
		err, _ := manager.Reconcile(ctx, config.FullConfig{Services: emptyConfig}, 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(manager.GetInstances()).To(BeEmpty(), "Should start with no services")

		// 2. Create config with one service to trigger creation and startup
		GinkgoWriter.Printf("\n=== PHASE 1: CREATING SERVICE ===\n")
		configWithService := []config.S6FSMConfig{
			{
				FSMInstanceConfig: config.FSMInstanceConfig{
					Name:            serviceName,
					DesiredFSMState: OperationalStateRunning,
				},
				S6ServiceConfig: config.S6ServiceConfig{
					Command:     []string{"/bin/sh", "-c", "echo test-service"},
					Env:         map[string]string{"TEST": "value"},
					ConfigFiles: map[string]string{},
				},
			},
		}

		// Reconcile once to let the instance be created
		GinkgoWriter.Printf("Reconciling once to set up mock service\n")
		err, _ = manager.Reconcile(ctx, config.FullConfig{Services: configWithService}, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(manager.GetInstances()).To(HaveKey(serviceName), "Service should be created")

		// Get the instance and replace its service with our mock
		instance, ok := manager.GetInstance(serviceName)
		Expect(ok).To(BeTrue())
		s6Instance, ok := instance.(*S6Instance)
		Expect(ok).To(BeTrue())
		s6Instance.service = mockService

		servicePath := s6Instance.servicePath

		// Make sure we're using the correct path in our mock
		mockService.ExistingServices[servicePath] = true
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}
		mockService.GetConfigResult = config.S6ServiceConfig{
			Command:     []string{"/bin/sh", "-c", "echo test-service"},
			Env:         map[string]string{"TEST": "value"},
			ConfigFiles: map[string]string{},
		}

		// Loop to bring service to running state
		maxCreationAttempts := 10

		for i := 1; i <= maxCreationAttempts; i++ {
			// Reconcile with the service config
			GinkgoWriter.Printf("\n--- Creation reconcile #%d ---\n", i)
			err, _ = manager.Reconcile(ctx, config.FullConfig{Services: configWithService}, uint64(1+i))
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
			if s6Instance, ok := instance.(*S6Instance); ok {
				if s6Instance.baseFSMInstance.GetError() != nil {
					GinkgoWriter.Printf("FSM Error: %v\n", s6Instance.baseFSMInstance.GetError())
				}
			} else {
				Fail("instance is not an S6Instance")
			}
		}

		// Verify the service reached running state
		serviceRunning := instance.GetCurrentFSMState() == OperationalStateRunning
		Expect(serviceRunning).To(BeTrue(), "Service should have reached running state")
		Expect(manager.GetInstances()).To(HaveKey(serviceName))
		Expect(manager.GetInstances()[serviceName].GetCurrentFSMState()).To(Equal(OperationalStateRunning))

		// 3. Now remove the service by using empty config again
		GinkgoWriter.Printf("\n=== PHASE 2: REMOVING SERVICE ===\n")

		// Get reference to instance before removal
		instance, ok = manager.GetInstance(serviceName)
		Expect(ok).To(BeTrue())
		currentState := instance.GetCurrentFSMState()
		desiredState := instance.GetDesiredFSMState()
		serviceStatus := string(mockService.ServiceStates[servicePath].Status)

		GinkgoWriter.Printf("Before removal: Current=%s, Desired=%s, ServiceStatus=%s\n",
			currentState, desiredState, serviceStatus)

		// Loop to remove service
		maxRemovalAttempts := 15
		serviceRemoved := false

		for i := 1; i <= maxRemovalAttempts; i++ {
			// Reconcile with empty config
			GinkgoWriter.Printf("\n--- Removal reconcile #%d ---\n", i)
			err, _ = manager.Reconcile(ctx, config.FullConfig{Services: emptyConfig}, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())

			// Log state after reconciliation
			if instance, exists := manager.GetInstance(serviceName); exists {
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
				if s6Instance, ok := instance.(*S6Instance); ok {
					if s6Instance.baseFSMInstance.GetError() != nil {
						GinkgoWriter.Printf("FSM Error: %v\n", s6Instance.baseFSMInstance.GetError())
					}
				} else {
					Fail("instance is not an S6Instance")
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
		Expect(manager.GetInstances()).NotTo(HaveKey(serviceName))
	})

	It("should transition a service from stopped to running when the desired state is changed in the config", func() {
		// Create a mock service
		mockService := s6service.NewMockService()
		serviceName := "test-state-change"

		// Create a config with the service desired state as stopped
		configWithStoppedService := []config.S6FSMConfig{
			{
				FSMInstanceConfig: config.FSMInstanceConfig{
					Name:            serviceName,
					DesiredFSMState: OperationalStateStopped,
				},
				S6ServiceConfig: config.S6ServiceConfig{
					Command:     []string{"/bin/sh", "-c", "echo test"},
					Env:         map[string]string{},
					ConfigFiles: map[string]string{},
				},
			},
		}

		mockService.GetConfigResult = config.S6ServiceConfig{
			Command:     []string{"/bin/sh", "-c", "echo test"},
			Env:         map[string]string{},
			ConfigFiles: map[string]string{},
		}

		// Use createMockS6Instance which properly sets up the instance and mock service
		instance := createMockS6Instance(manager, serviceName, mockService, OperationalStateStopped)
		servicePath := instance.servicePath

		// Set up the mock service as already created and stopped
		configureServiceForState(mockService, servicePath, OperationalStateStopped)

		// Force the state to be stopped (skip creation lifecycle)
		setS6InstanceState(instance, OperationalStateStopped)

		// Verify initial state is correctly set
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped), "Initial state should be stopped")
		Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateStopped), "Initial desired state should be stopped")
		Expect(instance.ObservedState.ServiceInfo.Status).To(Equal(s6service.ServiceDown), "Service status should be down")

		// Reconcile to ensure state is stable
		err, _ := manager.Reconcile(ctx, config.FullConfig{Services: configWithStoppedService}, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped), "State should remain stopped after reconcile")

		// Now modify the config to change the desired state to running
		configWithRunningService := []config.S6FSMConfig{
			{
				FSMInstanceConfig: config.FSMInstanceConfig{
					Name:            serviceName,
					DesiredFSMState: OperationalStateRunning,
				},
				S6ServiceConfig: config.S6ServiceConfig{
					Command:     []string{"/bin/sh", "-c", "echo test"},
					Env:         map[string]string{},
					ConfigFiles: map[string]string{},
				},
			},
		}

		// Reconcile with the updated config
		GinkgoWriter.Printf("\n=== Changing desired state to running ===\n")
		err, _ = manager.Reconcile(ctx, config.FullConfig{Services: configWithRunningService}, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())

		// Verify the desired state changed but current state should still be stopped
		Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateRunning), "Desired state should now be running")
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped), "Current state should still be stopped")

		// First reconcile should transition to starting
		GinkgoWriter.Printf("First reconcile after state change - should transition to starting\n")
		err, _ = instance.Reconcile(ctx, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting), "State should transition to starting")
		Expect(mockService.StartCalled).To(BeTrue(), "Service.Start() should be called")

		// Update the mock service state to running
		configureServiceForState(mockService, servicePath, OperationalStateRunning)

		// Second reconcile should transition to running
		GinkgoWriter.Printf("Second reconcile after state change - should transition to running\n")
		err, _ = instance.Reconcile(ctx, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateRunning), "State should transition to running")

		// Verify final state
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateRunning))
		Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateRunning))
		Expect(instance.ObservedState.ServiceInfo.Status).To(Equal(s6service.ServiceUp))
	})

	It("should transition a service from running to stopped when the desired state is changed in the config", func() {
		// Create a mock service
		mockService := s6service.NewMockService()
		serviceName := "test-stopping"

		// Use createMockS6Instance which properly sets up the instance and mock service
		instance := createMockS6Instance(manager, serviceName, mockService, OperationalStateRunning)
		servicePath := instance.servicePath

		// Set up the mock service as already created and running
		configureServiceForState(mockService, servicePath, OperationalStateRunning)

		// Force the state to be running (skip creation lifecycle)
		setS6InstanceState(instance, OperationalStateRunning)

		// Verify initial state is correctly set
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateRunning), "Initial state should be running")
		Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateRunning), "Initial desired state should be running")

		// Also verify the observed state is correctly set
		Expect(instance.ObservedState.ServiceInfo.Status).To(Equal(s6service.ServiceUp), "Service status should be up")

		// Create the running config to ensure desired state is consistently set
		configWithRunningService := []config.S6FSMConfig{
			{
				FSMInstanceConfig: config.FSMInstanceConfig{
					Name:            serviceName,
					DesiredFSMState: OperationalStateRunning,
				},
				S6ServiceConfig: config.S6ServiceConfig{
					Command:     []string{"/bin/sh", "-c", "echo test"},
					Env:         map[string]string{},
					ConfigFiles: map[string]string{},
				},
			},
		}

		mockService.GetConfigResult = config.S6ServiceConfig{
			Command:     []string{"/bin/sh", "-c", "echo test"},
			Env:         map[string]string{},
			ConfigFiles: map[string]string{},
		}

		// Reconcile to ensure state is stable
		err, _ := manager.Reconcile(ctx, config.FullConfig{Services: configWithRunningService}, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateRunning), "State should remain running after reconcile")

		// Now modify the config to change the desired state to stopped
		configWithStoppedService := []config.S6FSMConfig{
			{
				FSMInstanceConfig: config.FSMInstanceConfig{
					Name:            serviceName,
					DesiredFSMState: OperationalStateStopped,
				},
				S6ServiceConfig: config.S6ServiceConfig{
					Command:     []string{"/bin/sh", "-c", "echo test"},
					Env:         map[string]string{},
					ConfigFiles: map[string]string{},
				},
			},
		}

		// Reconcile with the updated config
		GinkgoWriter.Printf("\n=== Changing desired state to stopped ===\n")
		err, _ = manager.Reconcile(ctx, config.FullConfig{Services: configWithStoppedService}, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())

		// Verify the desired state changed but current state should still be running
		Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateStopped), "Desired state should now be stopped")
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateRunning), "Current state should still be running")

		// First reconcile should transition to stopping
		GinkgoWriter.Printf("First reconcile after state change - should transition to stopping\n")
		err, _ = instance.Reconcile(ctx, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopping), "State should transition to stopping")
		Expect(mockService.StopCalled).To(BeTrue(), "Service.Stop() should be called")

		// Update the mock service state to stopped
		configureServiceForState(mockService, servicePath, OperationalStateStopped)

		// Second reconcile should transition to stopped
		GinkgoWriter.Printf("Second reconcile after state change - should transition to stopped\n")
		err, _ = instance.Reconcile(ctx, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped), "State should transition to stopped")

		// Verify final state
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped))
		Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateStopped))
		Expect(instance.ObservedState.ServiceInfo.Status).To(Equal(s6service.ServiceDown))
	})

	It("should handle a service that stays in down state after attempting to start", func() {
		// Create a mock service
		mockService := s6service.NewMockService()
		serviceName := "test-slow-start"

		// Create a config with the service desired state as stopped
		configWithStoppedService := []config.S6FSMConfig{
			{
				FSMInstanceConfig: config.FSMInstanceConfig{
					Name:            serviceName,
					DesiredFSMState: OperationalStateStopped,
				},
				S6ServiceConfig: config.S6ServiceConfig{
					Command:     []string{"/bin/sh", "-c", "echo test"},
					Env:         map[string]string{},
					ConfigFiles: map[string]string{},
				},
			},
		}

		mockService.GetConfigResult = config.S6ServiceConfig{
			Command:     []string{"/bin/sh", "-c", "echo test"},
			Env:         map[string]string{},
			ConfigFiles: map[string]string{},
		}

		// Use createMockS6Instance which properly sets up the instance and mock service
		instance := createMockS6Instance(manager, serviceName, mockService, OperationalStateStopped)
		servicePath := instance.servicePath

		// Set up the mock service as already created and stopped
		configureServiceForState(mockService, servicePath, OperationalStateStopped)

		// Force the state to be stopped (skip creation lifecycle)
		setS6InstanceState(instance, OperationalStateStopped)

		// Verify initial state is correctly set
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped), "Initial state should be stopped")
		Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateStopped), "Initial desired state should be stopped")
		Expect(instance.ObservedState.ServiceInfo.Status).To(Equal(s6service.ServiceDown), "Service status should be down")

		// Reconcile 10 times to ensure state is stable
		for i := 0; i < 10; i++ {
			err, _ := manager.Reconcile(ctx, config.FullConfig{Services: configWithStoppedService}, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped), "State should remain stopped after reconcile")

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
		}

		// Now modify the config to change the desired state to running
		configWithRunningService := []config.S6FSMConfig{
			{
				FSMInstanceConfig: config.FSMInstanceConfig{
					Name:            serviceName,
					DesiredFSMState: OperationalStateRunning,
				},
				S6ServiceConfig: config.S6ServiceConfig{
					Command:     []string{"/bin/sh", "-c", "echo test"},
					Env:         map[string]string{},
					ConfigFiles: map[string]string{},
				},
			},
		}

		// Reconcile with the updated config
		GinkgoWriter.Printf("\n=== Changing desired state to running ===\n")
		err, _ := manager.Reconcile(ctx, config.FullConfig{Services: configWithRunningService}, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())

		// Verify the desired state changed but current state should still be stopped
		Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateRunning), "Desired state should now be running")
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped), "Current state should still be stopped")

		// First reconcile should transition to starting
		GinkgoWriter.Printf("Reconcile after state change - should transition to starting\n")
		err, _ = instance.Reconcile(ctx, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting), "State should transition to starting")
		Expect(mockService.StartCalled).To(BeTrue(), "Service.Start() should be called")

		// The service remains in "down" state even after start command
		// This simulates a slow-starting service
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}

		// Reconcile and check for 10 times that the state is still starting
		for i := 0; i < 10; i++ {
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting), "State should remain in starting")

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
		}

		// Now simulate the service becoming available (up)
		GinkgoWriter.Printf("Service now becomes available (up)\n")
		configureServiceForState(mockService, servicePath, OperationalStateRunning)

		// Next reconcile should detect the service is up and transition to running
		GinkgoWriter.Printf("Reconcile - service now up, should transition to running\n")
		err, _ = instance.Reconcile(ctx, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateRunning), "State should transition to running")

		// Verify final state
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateRunning))
		Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateRunning))
		Expect(instance.ObservedState.ServiceInfo.Status).To(Equal(s6service.ServiceUp))
	})

	It("should handle a service that stays up after attempting to stop", func() {
		// Create a mock service
		mockService := s6service.NewMockService()
		serviceName := "test-slow-stop"

		// Use createMockS6Instance which properly sets up the instance and mock service
		instance := createMockS6Instance(manager, serviceName, mockService, OperationalStateRunning)
		servicePath := instance.servicePath

		// Set up the mock service as already created and running
		configureServiceForState(mockService, servicePath, OperationalStateRunning)

		// Force the state to be running (skip creation lifecycle)
		setS6InstanceState(instance, OperationalStateRunning)

		// Verify initial state is correctly set
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateRunning), "Initial state should be running")
		Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateRunning), "Initial desired state should be running")

		// Also verify the observed state is correctly set
		Expect(instance.ObservedState.ServiceInfo.Status).To(Equal(s6service.ServiceUp), "Service status should be up")

		// Create the running config to ensure desired state is consistently set
		configWithRunningService := []config.S6FSMConfig{
			{
				FSMInstanceConfig: config.FSMInstanceConfig{
					Name:            serviceName,
					DesiredFSMState: OperationalStateRunning,
				},
				S6ServiceConfig: config.S6ServiceConfig{
					Command:     []string{"/bin/sh", "-c", "echo test"},
					Env:         map[string]string{},
					ConfigFiles: map[string]string{},
				},
			},
		}

		mockService.GetConfigResult = config.S6ServiceConfig{
			Command:     []string{"/bin/sh", "-c", "echo test"},
			Env:         map[string]string{},
			ConfigFiles: map[string]string{},
		}

		// Reconcile to ensure state is stable
		err, _ := manager.Reconcile(ctx, config.FullConfig{Services: configWithRunningService}, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateRunning), "State should remain running after reconcile")

		// Now modify the config to change the desired state to stopped
		configWithStoppedService := []config.S6FSMConfig{
			{
				FSMInstanceConfig: config.FSMInstanceConfig{
					Name:            serviceName,
					DesiredFSMState: OperationalStateStopped,
				},
				S6ServiceConfig: config.S6ServiceConfig{
					Command:     []string{"/bin/sh", "-c", "echo test"},
					Env:         map[string]string{},
					ConfigFiles: map[string]string{},
				},
			},
		}

		// Reconcile with the updated config
		GinkgoWriter.Printf("\n=== Changing desired state to stopped ===\n")
		err, _ = manager.Reconcile(ctx, config.FullConfig{Services: configWithStoppedService}, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())

		// Verify the desired state changed but current state should still be running
		Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateStopped), "Desired state should now be stopped")
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateRunning), "Current state should still be running")

		// First reconcile should transition to stopping
		GinkgoWriter.Printf("First reconcile after state change - should transition to stopping\n")
		err, _ = instance.Reconcile(ctx, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopping), "State should transition to stopping")
		Expect(mockService.StopCalled).To(BeTrue(), "Service.Stop() should be called")

		// The service remains in "up" state even after stop command
		// This simulates a slow-stopping service or one that ignores stop requests
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceUp,
			Uptime: 10,
			Pid:    12345,
		}

		// Reconcile and check for multiple cycles that the state remains in stopping
		// while the service is still reporting as up
		GinkgoWriter.Printf("Reconcile multiple times - service still up, should remain in stopping state\n")
		for i := 0; i < 5; i++ {
			mockService.StopCalled = false // Reset for next check
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopping), "State should remain in stopping")

			// Stop might be called again on subsequent reconciles
			GinkgoWriter.Printf("Iteration %d: Stop called: %v\n", i, mockService.StopCalled)

			// Check observed state is still up
			Expect(instance.ObservedState.ServiceInfo.Status).To(Equal(s6service.ServiceUp), "Service status should still be up")

			currentState := instance.GetCurrentFSMState()
			desiredState := instance.GetDesiredFSMState()

			GinkgoWriter.Printf("Current state: %s\n", currentState)
			GinkgoWriter.Printf("Desired state: %s\n", desiredState)
			GinkgoWriter.Printf("Service status: %s\n", string(instance.ObservedState.ServiceInfo.Status))
		}

		// Now simulate the service finally stopping
		GinkgoWriter.Printf("Service now becomes down\n")
		configureServiceForState(mockService, servicePath, OperationalStateStopped)

		// Next reconcile should detect the service is down and transition to stopped
		GinkgoWriter.Printf("Final reconcile - service now down, should transition to stopped\n")
		err, _ = instance.Reconcile(ctx, tick)
		tick++
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped), "State should transition to stopped")

		// Verify final state
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped))
		Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateStopped))
		Expect(instance.ObservedState.ServiceInfo.Status).To(Equal(s6service.ServiceDown))
	})
})
