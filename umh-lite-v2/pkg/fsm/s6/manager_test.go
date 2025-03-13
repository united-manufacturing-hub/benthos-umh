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

	It("should remove a service when it disappears from config", func() {
		// Create a mock service and add an instance
		mockService := s6service.NewMockService()
		serviceName := "test-remove"
		servicePath := filepath.Join("/mock/path", serviceName)

		// Create and setup service in stopped state
		instance := createMockS6Instance(manager, serviceName, mockService, OperationalStateStopped)

		// Set the current state to operational stopped (required for removal)
		instance.baseFSMInstance.SetCurrentFSMState(OperationalStateStopped)
		mockService.ExistingServices[servicePath] = true
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}

		// Verify the service is in the correct initial state
		Expect(manager.Instances).To(HaveLen(1))
		Expect(manager.Instances).To(HaveKey(serviceName))
		Expect(manager.Instances[serviceName].GetCurrentFSMState()).To(Equal(OperationalStateStopped))

		// Create empty config (without our service) to trigger removal
		emptyConfig := config.FullConfig{
			Services: []config.S6FSMConfig{},
		}

		// First reconcile with empty config - this should mark the instance for removal
		err := manager.Reconcile(ctx, emptyConfig)
		Expect(err).NotTo(HaveOccurred())

		// Should now be in Removed state
		// Removing state is an intermediate state and should not be observable, except when there is an error
		Expect(manager.Instances[serviceName].GetCurrentFSMState()).To(Equal(internal_fsm.LifecycleStateRemoved))

		// Keep reconciling until the service is removed or max attempts reached
		maxAttempts := 5
		for attempt := 0; attempt < maxAttempts; attempt++ {
			if len(manager.Instances) == 0 {
				// Service was removed, test passes
				break
			}

			// Another reconcile
			err = manager.Reconcile(ctx, emptyConfig)
			Expect(err).NotTo(HaveOccurred())
		}

		// After all reconciliation attempts, the service should be removed
		Expect(manager.Instances).To(BeEmpty(), "Service should be removed after reconciliation")
	})
})
