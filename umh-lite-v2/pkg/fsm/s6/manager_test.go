package s6

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"time"

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

	// Table-driven test for removal from different states
	DescribeTable("removing a service from different states",
		func(initialState string) {
			// Create a mock service
			mockService := s6service.NewMockService()
			serviceName := fmt.Sprintf("test-remove-%s", initialState)
			servicePath := filepath.Join("/mock/path", serviceName)

			// Create service instance
			instance := createMockS6Instance(manager, serviceName, mockService, OperationalStateRunning)

			// Set up the initial state
			setS6InstanceState(instance, initialState)
			configureServiceForState(mockService, servicePath, initialState)

			// Verify initial setup
			Expect(manager.Instances).To(HaveKey(serviceName))
			Expect(manager.Instances[serviceName].GetCurrentFSMState()).To(Equal(initialState))

			// Create empty config to trigger removal
			emptyConfig := config.FullConfig{
				Services: []config.S6FSMConfig{},
			}

			// Reconcile to initiate removal
			err := manager.Reconcile(ctx, emptyConfig)
			Expect(err).NotTo(HaveOccurred())

			// For non-stopped states we need to stop first, others can remove directly
			if initialState != OperationalStateStopped {
				// Verify service exists and is in appropriate state
				// Transitions might be different depending on initial state
				Expect(manager.Instances).To(HaveKey(serviceName))

				// If the service was already in removing state or is gone, that's fine
				// Otherwise it should be transitioning
				if manager.Instances[serviceName] != nil {
					state := manager.Instances[serviceName].GetCurrentFSMState()
					// The valid states after reconcile could be:
					// 1. Original state (waiting to stop)
					// 2. Stopping (in process of stopping)
					// 3. Stopped (successfully stopped, ready for removal)
					// 4. Removing (if already stopped)
					// 5. Removed (if already removed)
					validTransitionStates := []string{
						initialState,
						OperationalStateStopping,
						OperationalStateStopped,
						internal_fsm.LifecycleStateRemoving,
						internal_fsm.LifecycleStateRemoved,
					}

					Expect(validTransitionStates).To(ContainElement(state),
						"Expected state to be one of %v but got %s", validTransitionStates, state)

					// Help it transition to stopped if needed
					if state == initialState || state == OperationalStateStopping {
						// Simulate service stopping
						mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
							Status: s6service.ServiceDown,
						}

						if state != OperationalStateStopped {
							// Now set to stopped to help the transition
							setS6InstanceState(instance, OperationalStateStopped)
						}
					}
				}
			}

			// Keep reconciling until service is removed, with a max number of attempts
			maxAttempts := 5
			for i := 0; i < maxAttempts; i++ {
				// If service is gone, we're done
				if len(manager.Instances) == 0 || manager.Instances[serviceName] == nil {
					break
				}

				// If in removed state, one more reconcile should delete it
				if manager.Instances[serviceName].GetCurrentFSMState() == internal_fsm.LifecycleStateRemoved {
					err = manager.Reconcile(ctx, emptyConfig)
					Expect(err).NotTo(HaveOccurred())
					break
				}

				// Another reconcile cycle
				err = manager.Reconcile(ctx, emptyConfig)
				Expect(err).NotTo(HaveOccurred())

				// Short delay to avoid issues
				time.Sleep(time.Millisecond * 10)
			}

			// After all reconciliations, the instance should be removed
			Expect(manager.Instances).NotTo(HaveKey(serviceName),
				"Service should be removed after reconciliation from state %s", initialState)
		},
		Entry("from stopped state", OperationalStateStopped),
		Entry("from running state", OperationalStateRunning),
		Entry("from starting state", OperationalStateStarting),
		Entry("from stopping state", OperationalStateStopping),
	)

	// Random state transition test
	FIt("should handle random state transitions and removal reliably", func() {
		// Use fixed seed for reproducibility
		rand.Seed(42)

		// Create several services
		mockService := s6service.NewMockService()

		// Number of services to test with
		numServices := 5
		serviceNames := make([]string, numServices)
		servicePaths := make([]string, numServices)

		// Create services with random initial states
		possibleStates := []string{
			OperationalStateStopped,
			OperationalStateRunning,
			OperationalStateStarting,
			OperationalStateStopping,
		}

		// Create services
		for i := 0; i < numServices; i++ {
			serviceNames[i] = fmt.Sprintf("fuzz-test-service-%d", i)
			servicePaths[i] = filepath.Join("/mock/path", serviceNames[i])

			instance := createMockS6Instance(manager, serviceNames[i], mockService, OperationalStateRunning)

			// Set random initial state
			initialState := possibleStates[rand.Intn(len(possibleStates))]
			setS6InstanceState(instance, initialState)
			configureServiceForState(mockService, servicePaths[i], initialState)
		}

		// Verify all services exist
		Expect(manager.Instances).To(HaveLen(numServices))

		// Create empty config to trigger removal of all services
		emptyConfig := config.FullConfig{
			Services: []config.S6FSMConfig{},
		}

		// Reconcile multiple times, with random actions between reconciles
		maxIterations := 15
		for i := 0; i < maxIterations; i++ {
			// Perform reconciliation
			err := manager.Reconcile(ctx, emptyConfig)
			Expect(err).NotTo(HaveOccurred())

			// For remaining services, randomly change their states to simulate chaotic conditions
			for name, instance := range manager.Instances {
				if rand.Float32() < 0.3 { // 30% chance
					// Pick a random state
					newState := possibleStates[rand.Intn(len(possibleStates))]
					setS6InstanceState(instance, newState)
					configureServiceForState(mockService, instance.servicePath, newState)

					// Log the transition for debugging
					GinkgoWriter.Printf("Iteration %d: Changed service %s to state %s\n",
						i, name, newState)
				}
			}
		}

		// After multiple iterations, all services should be removed
		Expect(manager.Instances).To(BeEmpty(),
			"All services should eventually be removed after multiple reconciliations")
	})
})
