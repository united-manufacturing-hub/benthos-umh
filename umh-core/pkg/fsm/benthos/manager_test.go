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

			// We need to transition from degraded to stopped before we can remove
			// First, get the instance
			instance, found := manager.GetInstance(serviceName)
			Expect(found).To(BeTrue(), "Service should exist before removal")

			// Update mock service state to allow transitioning to stopped
			setupServiceState(mockService, serviceName, benthossvc.ServiceStateFlags{
				IsS6Running:          false,
				S6FSMState:           s6fsm.OperationalStateStopped,
				IsConfigLoaded:       false,
				IsHealthchecksPassed: false,
			})

			// Reconcile to move to stopped state
			err, _ = manager.Reconcile(ctx, emptyConfig, tick)
			Expect(err).NotTo(HaveOccurred())
			tick += 5

			// Wait for service to reach stopped state by reconciling multiple times
			maxStopAttempts := 20
			for attempt := 0; attempt < maxStopAttempts; attempt++ {
				err, _ = manager.Reconcile(ctx, emptyConfig, tick)
				Expect(err).NotTo(HaveOccurred())
				tick += 5

				// Check if service has reached stopped state
				if instance, found := manager.GetInstance(serviceName); found {
					if instance.GetCurrentFSMState() == OperationalStateStopped {
						break
					}
					GinkgoWriter.Printf("Service %s currently in state %s (attempt %d/%d)\n",
						serviceName, instance.GetCurrentFSMState(), attempt+1, maxStopAttempts)
				} else {
					GinkgoWriter.Printf("Service %s not found (attempt %d/%d)\n", serviceName, attempt+1, maxStopAttempts)
					break
				}

				// If we reach max attempts, fail the test
				if attempt == maxStopAttempts-1 {
					Fail(fmt.Sprintf("Service did not reach stopped state after %d attempts", maxStopAttempts))
				}
			}

			// Verify we're in stopped state before proceeding
			instance, found = manager.GetInstance(serviceName)
			Expect(found).To(BeTrue(), "Service should exist after transitioning to stopped")
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped),
				"Service should be in stopped state before removal")

			// Now that it's stopped, we can remove it
			err = instance.Remove(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Check removal with reconciliation multiple times
			maxRemoveAttempts := 20
			for attempt := 0; attempt < maxRemoveAttempts; attempt++ {
				// Reconcile and advance tick
				err, _ = manager.Reconcile(ctx, emptyConfig, tick)
				Expect(err).NotTo(HaveOccurred())
				tick += 20 // Use larger increments to bypass rate limiting

				// Check if service has been removed
				if len(manager.GetInstances()) == 0 {
					GinkgoWriter.Printf("Service %s successfully removed after %d attempts\n",
						serviceName, attempt+1)
					break
				}

				// Print status for debugging
				if instance, found := manager.GetInstance(serviceName); found {
					GinkgoWriter.Printf("Service %s still exists in state %s (attempt %d/%d)\n",
						serviceName, instance.GetCurrentFSMState(), attempt+1, maxRemoveAttempts)
				} else {
					GinkgoWriter.Printf("Service no longer in manager but instances not cleared (attempt %d/%d)\n",
						attempt+1, maxRemoveAttempts)
				}

				// If we reach max attempts, fail the test
				if attempt == maxRemoveAttempts-1 {
					Fail(fmt.Sprintf("Service was not removed after %d attempts", maxRemoveAttempts))
				}
			}

			// Final verification
			Expect(manager.GetInstances()).To(BeEmpty(), "All services should be removed")
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

			// Should move to config loading state after S6 is running
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStartingConfigLoading))

			// Now fail the service by stopping S6
			setupServiceState(mockService, serviceName, benthossvc.ServiceStateFlags{
				IsS6Running:          false, // This is what triggers EventStartFailed
				S6FSMState:           s6fsm.OperationalStateStopped,
				IsConfigLoaded:       false,
				IsHealthchecksPassed: false,
			})

			// This should trigger the transition back to starting
			err, _ = manager.Reconcile(ctx, fullConfig, tick)
			Expect(err).NotTo(HaveOccurred())
			tick++

			// Should have gone back to starting state immediately
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting),
				"Service should go back to starting state when S6 fails")

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

		It("should handle 'service not found' errors gracefully", func() {
			serviceName := "nonexistent-service"

			// First, let's verify that requesting status for a non-existent service returns the right error
			_, err := mockService.Status(ctx, serviceName, 0, tick)
			Expect(err).To(Equal(benthossvc.ErrServiceNotExist))

			// Now add the service to the config but don't actually create it yet
			benthosConfig := createBenthosConfig(serviceName, OperationalStateActive)
			fullConfig := config.FullConfig{
				Benthos: []config.BenthosConfig{benthosConfig},
			}

			// The first reconcile should not fail, but should trigger service creation
			err, reconciled := manager.Reconcile(ctx, fullConfig, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciled).To(BeTrue(), "Reconciliation should have happened")
			tick++

			// The S6 service should now be in the creation process
			_, found := manager.GetInstance(serviceName)
			Expect(found).To(BeTrue(), "Service instance should be created")

			// Let's pretend the S6 service doesn't exist yet (simulating a creation in progress)
			delete(mockService.ExistingServices, serviceName)

			// Another reconcile should still succeed because our error handling is robust
			err, _ = manager.Reconcile(ctx, fullConfig, tick)
			Expect(err).NotTo(HaveOccurred(), "Reconcile should handle service not found gracefully")

			// Now let's make the service available
			mockService.ExistingServices[serviceName] = true

			// Make sure the service is initialized properly
			setupServiceState(mockService, serviceName, benthossvc.ServiceStateFlags{
				IsS6Running:            true,
				S6FSMState:             s6fsm.OperationalStateRunning,
				IsConfigLoaded:         true,
				IsHealthchecksPassed:   true,
				IsRunningWithoutErrors: true,
			})

			// Wait for service to reach idle state
			maxAttemptsIdle := 20
			for attempt := 0; attempt < maxAttemptsIdle; attempt++ {
				err, _ = manager.Reconcile(ctx, fullConfig, tick)
				Expect(err).NotTo(HaveOccurred())
				tick += 5

				// Check if service has reached idle state
				if instance, found := manager.GetInstance(serviceName); found {
					if instance.GetCurrentFSMState() == OperationalStateIdle {
						break
					}
					GinkgoWriter.Printf("Service %s currently in state %s (attempt %d/%d)\n",
						serviceName, instance.GetCurrentFSMState(), attempt+1, maxAttemptsIdle)
				} else {
					GinkgoWriter.Printf("Service %s not found (attempt %d/%d)\n", serviceName, attempt+1, maxAttemptsIdle)
					break
				}

				// If we reach max attempts, fail the test
				if attempt == maxAttemptsIdle-1 {
					Fail(fmt.Sprintf("Service did not reach idle state after %d attempts", maxAttemptsIdle))
				}
			}

			// Verify final state
			instance, found := manager.GetInstance(serviceName)
			Expect(found).To(BeTrue(), "Service should exist at end of test")
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateIdle), "Service should be in idle state")
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
