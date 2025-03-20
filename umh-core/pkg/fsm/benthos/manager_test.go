package benthos

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	s6fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm/s6"
	benthossvc "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/benthos"
)

// createMockBenthosManagerInstance creates a BenthosInstance specifically for the manager tests
func createMockBenthosManagerInstance(name string, mockService benthossvc.IBenthosService, desiredState string) *BenthosInstance {
	cfg := config.BenthosConfig{
		FSMInstanceConfig: config.FSMInstanceConfig{
			Name:            name,
			DesiredFSMState: desiredState,
		},
		BenthosServiceConfig: config.BenthosServiceConfig{},
	}

	instance := NewBenthosInstance(baseBenthosDir, cfg)
	// Replace the service with our mock
	instance.service = mockService
	return instance
}

// createMockBenthosManager creates a BenthosManager with a mock service for testing
func createMockBenthosManager(name string) (*BenthosManager, *benthossvc.MockBenthosService) {
	mockService := benthossvc.NewMockBenthosService()
	manager := NewBenthosManager(name)
	return manager, mockService
}

// createBenthosConfig creates a test BenthosConfig with the given name and desired state
func createBenthosConfig(name, desiredState string) config.BenthosConfig {
	return config.BenthosConfig{
		FSMInstanceConfig: config.FSMInstanceConfig{
			Name:            name,
			DesiredFSMState: desiredState,
		},
		BenthosServiceConfig: config.BenthosServiceConfig{},
	}
}

// setupServiceState configures the mock service state for a given service
func setupServiceState(mockService *benthossvc.MockBenthosService, serviceName string, flags benthossvc.ServiceStateFlags) {
	mockService.SetServiceState(serviceName, flags)
	mockService.ServiceStates[serviceName].BenthosStatus.HealthCheck = benthossvc.HealthCheck{
		IsLive:  flags.IsHealthchecksPassed,
		IsReady: flags.IsHealthchecksPassed,
	}

	// Set up metrics state if service is processing
	if flags.HasProcessingActivity {
		mockService.ServiceStates[serviceName].BenthosStatus.MetricsState = &benthossvc.BenthosMetricsState{
			IsActive: true,
		}
	}

	// Set up S6 observed state
	if flags.IsS6Running {
		mockService.ServiceStates[serviceName].S6ObservedState.ServiceInfo.Uptime = 15
	}

	// Mark the service as existing in the mock service
	mockService.ExistingServices[serviceName] = true

	// Setup default config results
	mockService.GetConfigResult = config.BenthosServiceConfig{}
}

// setupServiceInManager adds a service to the manager with the mock instance
func setupServiceInManager(manager *BenthosManager, mockService benthossvc.IBenthosService, serviceName string, desiredState string) {
	instance := createMockBenthosManagerInstance(serviceName, mockService, desiredState)
	manager.BaseFSMManager.AddInstanceForTest(serviceName, instance)
}

// waitForState repeatedly reconciles until the desired state is reached or maxAttempts is exceeded
func waitForState(ctx context.Context, manager *BenthosManager, config config.FullConfig, startTick uint64, desiredState string, maxAttempts int) (uint64, error) {
	tick := startTick
	var lastErr error

	for i := 0; i < maxAttempts; i++ {
		lastErr, _ = manager.Reconcile(ctx, config, tick)
		if lastErr != nil {
			return tick, lastErr
		}

		// Check if all instances have reached the desired state
		allInDesiredState := true
		for _, instance := range manager.GetInstances() {
			if instance.GetCurrentFSMState() != desiredState {
				allInDesiredState = false
				break
			}
		}

		if allInDesiredState {
			return tick, nil
		}

		tick++
	}

	return tick, fmt.Errorf("failed to reach state %s after %d attempts", desiredState, maxAttempts)
}

// transitionToIdle helps transition a service through all startup states to reach idle
func transitionToIdle(ctx context.Context, manager *BenthosManager, mockService *benthossvc.MockBenthosService, serviceName string, config config.FullConfig, startTick uint64) (uint64, error) {
	tick := startTick

	// Add the instance to the manager
	setupServiceInManager(manager, mockService, serviceName, OperationalStateActive)

	// Setup initial mock service response for creation
	mockService.ExistingServices[serviceName] = true

	// Setup service as created but stopped
	setupServiceState(mockService, serviceName, benthossvc.ServiceStateFlags{
		IsS6Running: false,
		S6FSMState:  s6fsm.OperationalStateStopped,
	})

	// First reconcile to update instance state
	err, _ := manager.Reconcile(ctx, config, tick)
	if err != nil {
		return tick, fmt.Errorf("failed initial reconcile: %w", err)
	}
	tick++

	// Wait for instance to reach stopped state
	tick, err = waitForState(ctx, manager, config, tick, OperationalStateStopped, 10)
	if err != nil {
		return tick, fmt.Errorf("failed to reach stopped state: %w", err)
	}

	// Configure service for starting phase
	setupServiceState(mockService, serviceName, benthossvc.ServiceStateFlags{
		IsS6Running:            true,
		S6FSMState:             s6fsm.OperationalStateRunning,
		IsConfigLoaded:         true,
		IsHealthchecksPassed:   true,
		IsRunningWithoutErrors: true,
	})

	// Wait for instance to reach idle state
	tick, err = waitForState(ctx, manager, config, tick, OperationalStateIdle, 20)
	if err != nil {
		return tick, fmt.Errorf("failed to reach idle state: %w", err)
	}

	return tick, nil
}

var _ = Describe("BenthosManager", func() {
	var (
		manager     *BenthosManager
		mockService *benthossvc.MockBenthosService
		ctx         context.Context
		tick        uint64
	)

	BeforeEach(func() {
		manager, mockService = createMockBenthosManager("test-manager")
		ctx = context.Background()
		tick = uint64(0)

		// Reset the mock service state
		mockService.ExistingServices = make(map[string]bool)
		mockService.ServiceStates = make(map[string]*benthossvc.ServiceInfo)
	})

	Context("Basic Operations", func() {
		It("should handle empty config without errors", func() {
			emptyConfig := config.FullConfig{
				Benthos: []config.BenthosConfig{},
			}

			err, _ := manager.Reconcile(ctx, emptyConfig, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(manager.GetInstances()).To(BeEmpty())
		})

		It("should manage a service that's manually added", func() {
			serviceName := "test-service"
			var err error

			// Setup the instance
			setupServiceInManager(manager, mockService, serviceName, OperationalStateActive)
			setupServiceState(mockService, serviceName, benthossvc.ServiceStateFlags{
				IsS6Running:            true,
				S6FSMState:             s6fsm.OperationalStateRunning,
				IsConfigLoaded:         true,
				IsHealthchecksPassed:   true,
				IsRunningWithoutErrors: true,
			})

			// Create config that references this service
			benthosConfig := createBenthosConfig(serviceName, OperationalStateActive)
			fullConfig := config.FullConfig{
				Benthos: []config.BenthosConfig{benthosConfig},
			}

			// Wait for service to reach idle state
			tick, err = waitForState(ctx, manager, fullConfig, tick, OperationalStateIdle, 20)
			Expect(err).NotTo(HaveOccurred())

			// Verify instance state
			instance, exists := manager.GetInstance(serviceName)
			Expect(exists).To(BeTrue())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateIdle))
			Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateActive))
		})
	})

	Context("Service Lifecycle", func() {
		It("should transition through all states and handle removal", func() {
			serviceName := "test-lifecycle"
			benthosConfig := createBenthosConfig(serviceName, OperationalStateActive)
			fullConfig := config.FullConfig{
				Benthos: []config.BenthosConfig{benthosConfig},
			}

			// Get to idle state first
			tick, err := transitionToIdle(ctx, manager, mockService, serviceName, fullConfig, tick)
			Expect(err).NotTo(HaveOccurred())

			// Setup for transition to active
			setupServiceState(mockService, serviceName, benthossvc.ServiceStateFlags{
				IsS6Running:            true,
				S6FSMState:             s6fsm.OperationalStateRunning,
				IsConfigLoaded:         true,
				IsHealthchecksPassed:   true,
				IsRunningWithoutErrors: true,
				HasProcessingActivity:  true,
			})

			// Wait for active state
			tick, err = waitForState(ctx, manager, fullConfig, tick, OperationalStateActive, 10)
			Expect(err).NotTo(HaveOccurred())

			// Simulate degradation
			setupServiceState(mockService, serviceName, benthossvc.ServiceStateFlags{
				IsS6Running:            true,
				S6FSMState:             s6fsm.OperationalStateRunning,
				IsConfigLoaded:         true,
				IsHealthchecksPassed:   false,
				IsRunningWithoutErrors: false,
				HasProcessingActivity:  true,
			})

			// Wait for degraded state
			tick, err = waitForState(ctx, manager, fullConfig, tick, OperationalStateDegraded, 10)
			Expect(err).NotTo(HaveOccurred())

			// Now remove the service from config
			emptyConfig := config.FullConfig{
				Benthos: []config.BenthosConfig{},
			}

			// Reconcile with empty config should trigger removal
			err, _ = manager.Reconcile(ctx, emptyConfig, tick)
			Expect(err).NotTo(HaveOccurred())

			// Service should eventually be removed
			Eventually(func() int {
				err, _ = manager.Reconcile(ctx, emptyConfig, tick)
				tick++
				return len(manager.GetInstances())
			}).Should(BeZero())
		})
	})

	Context("Error Handling", func() {
		It("should handle startup failures and retry appropriately", func() {
			serviceName := "test-error"
			benthosConfig := createBenthosConfig(serviceName, OperationalStateActive)
			fullConfig := config.FullConfig{
				Benthos: []config.BenthosConfig{benthosConfig},
			}

			// Add the instance directly
			setupServiceInManager(manager, mockService, serviceName, OperationalStateActive)

			// Setup initial failed state
			setupServiceState(mockService, serviceName, benthossvc.ServiceStateFlags{
				IsS6Running:          false,
				S6FSMState:           s6fsm.OperationalStateStopped,
				IsConfigLoaded:       false,
				IsHealthchecksPassed: false,
			})

			// Should stay in starting state while S6 is not running
			tick, err := waitForState(ctx, manager, fullConfig, tick, OperationalStateStarting, 5)
			Expect(err).NotTo(HaveOccurred())

			// Simulate S6 starting but config failing to load
			setupServiceState(mockService, serviceName, benthossvc.ServiceStateFlags{
				IsS6Running:          true,
				S6FSMState:           s6fsm.OperationalStateRunning,
				IsConfigLoaded:       false,
				IsHealthchecksPassed: false,
			})

			// Should move to config loading state
			instance, exists := manager.GetInstance(serviceName)
			Expect(exists).To(BeTrue())

			err, _ = manager.Reconcile(ctx, fullConfig, tick)
			Expect(err).NotTo(HaveOccurred())
			tick++

			// Should go back to starting state when config fails to load
			Eventually(func() string {
				err, _ = manager.Reconcile(ctx, fullConfig, tick)
				tick++
				return instance.GetCurrentFSMState()
			}).Should(Equal(OperationalStateStarting))

			// Finally let it succeed
			setupServiceState(mockService, serviceName, benthossvc.ServiceStateFlags{
				IsS6Running:            true,
				S6FSMState:             s6fsm.OperationalStateRunning,
				IsConfigLoaded:         true,
				IsHealthchecksPassed:   true,
				IsRunningWithoutErrors: true,
			})

			// Should reach idle state
			tick, err = waitForState(ctx, manager, fullConfig, tick, OperationalStateIdle, 20)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Multiple Services", func() {
		It("should manage multiple services independently", func() {
			service1Name := "test-service-1"
			service2Name := "test-service-2"

			// Create configs for both services
			config1 := createBenthosConfig(service1Name, OperationalStateActive)
			config2 := createBenthosConfig(service2Name, OperationalStateActive)
			fullConfig := config.FullConfig{
				Benthos: []config.BenthosConfig{config1, config2},
			}

			// Add both services to the manager
			setupServiceInManager(manager, mockService, service1Name, OperationalStateActive)
			setupServiceInManager(manager, mockService, service2Name, OperationalStateActive)

			// Setup service 1 for success
			setupServiceState(mockService, service1Name, benthossvc.ServiceStateFlags{
				IsS6Running:            true,
				S6FSMState:             s6fsm.OperationalStateRunning,
				IsConfigLoaded:         true,
				IsHealthchecksPassed:   true,
				IsRunningWithoutErrors: true,
			})

			// Setup service 2 for failure
			setupServiceState(mockService, service2Name, benthossvc.ServiceStateFlags{
				IsS6Running:          false,
				S6FSMState:           s6fsm.OperationalStateStopped,
				IsConfigLoaded:       false,
				IsHealthchecksPassed: false,
			})

			// First reconcile to update states
			err, _ := manager.Reconcile(ctx, fullConfig, tick)
			Expect(err).NotTo(HaveOccurred())
			tick++

			// Verify services are in different states
			instance1, exists := manager.GetInstance(service1Name)
			Expect(exists).To(BeTrue())
			Expect(instance1.GetCurrentFSMState()).To(Equal(OperationalStateStarting))

			instance2, exists := manager.GetInstance(service2Name)
			Expect(exists).To(BeTrue())
			Expect(instance2.GetCurrentFSMState()).To(Equal(OperationalStateStarting))

			// Let service 1 progress
			tick, err = waitForState(ctx, manager, fullConfig, tick, OperationalStateStartingConfigLoading, 5)
			Expect(err).NotTo(HaveOccurred())

			// Now let service 2 succeed
			setupServiceState(mockService, service2Name, benthossvc.ServiceStateFlags{
				IsS6Running:            true,
				S6FSMState:             s6fsm.OperationalStateRunning,
				IsConfigLoaded:         true,
				IsHealthchecksPassed:   true,
				IsRunningWithoutErrors: true,
			})

			// Both services should eventually reach idle state
			tick, err = waitForState(ctx, manager, fullConfig, tick, OperationalStateIdle, 20)
			Expect(err).NotTo(HaveOccurred())

			// Verify both services are in idle state
			instance1, _ = manager.GetInstance(service1Name)
			instance2, _ = manager.GetInstance(service2Name)
			Expect(instance1.GetCurrentFSMState()).To(Equal(OperationalStateIdle))
			Expect(instance2.GetCurrentFSMState()).To(Equal(OperationalStateIdle))
		})
	})
})
