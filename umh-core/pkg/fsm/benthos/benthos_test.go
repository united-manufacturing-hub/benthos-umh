package benthos

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	internalfsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	s6fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm/s6"
	benthossvc "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/benthos"
	s6svc "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/s6"
)

// createMockBenthosInstance creates a BenthosInstance with a mock service for testing
func createMockBenthosInstance(name string, mockService benthossvc.IBenthosService) *BenthosInstance {
	cfg := config.BenthosConfig{
		FSMInstanceConfig: config.FSMInstanceConfig{
			Name: name,
		},
		BenthosServiceConfig: config.BenthosServiceConfig{},
	}

	instance := NewBenthosInstance("/run/service", cfg)
	instance.service = mockService
	return instance
}

// Helper function to get an instance in Idle state
func getInstanceInIdleState(testID string, mockService *benthossvc.MockBenthosService, instance *BenthosInstance, ctx context.Context, tick uint64) (uint64, error) {
	// 1. Lifecycle creation phase
	// Initial state: to_be_created
	if err, _ := instance.Reconcile(ctx, tick); err != nil {
		return tick, err
	}
	tick++

	// Mock service creation success
	mockService.ServiceStates[testID] = &benthossvc.ServiceInfo{
		BenthosStatus: benthossvc.BenthosStatus{
			HealthCheck: benthossvc.HealthCheck{
				IsLive:  false,
				IsReady: false,
			},
		},
		S6FSMState: s6fsm.OperationalStateStopped,
	}

	// Second reconcile: created -> stopped
	if err, _ := instance.Reconcile(ctx, tick); err != nil {
		return tick, err
	}
	tick++

	// 2. Activate the instance
	if err := instance.SetDesiredFSMState(OperationalStateActive); err != nil {
		return tick, err
	}

	// 3. Starting phase
	// First operational reconcile: starting
	if err, _ := instance.Reconcile(ctx, tick); err != nil {
		return tick, err
	}
	tick++

	// 4. ConfigLoading phase
	mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
		IsS6Running:          true,
		S6FSMState:           s6fsm.OperationalStateRunning,
		IsConfigLoaded:       true,
		IsHealthchecksPassed: true,
	})
	mockService.GetConfigResult = config.BenthosServiceConfig{}

	// Add this line to simulate config validation
	mockService.ServiceStates[testID].S6ObservedState.ServiceInfo.Uptime = 5

	// Update health check status
	mockService.ServiceStates[testID].BenthosStatus.HealthCheck = benthossvc.HealthCheck{
		IsLive:  true,
		IsReady: true,
	}

	if err, _ := instance.Reconcile(ctx, tick); err != nil {
		return tick, err
	}
	tick++

	// 5. WaitingForHealthchecks phase
	mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
		IsS6Running:          true,
		S6FSMState:           s6fsm.OperationalStateRunning,
		IsConfigLoaded:       true,
		IsHealthchecksPassed: true,
	})
	mockService.ServiceStates[testID].BenthosStatus.HealthCheck = benthossvc.HealthCheck{
		IsLive:  true,
		IsReady: true,
	}

	if err, _ := instance.Reconcile(ctx, tick); err != nil {
		return tick, err
	}
	tick++

	// 6. WaitingForServiceToRemainRunning phase
	mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
		IsS6Running:            true,
		S6FSMState:             s6fsm.OperationalStateRunning,
		IsConfigLoaded:         true,
		IsHealthchecksPassed:   true,
		IsRunningWithoutErrors: true,
	})
	mockService.ServiceStates[testID].BenthosStatus.HealthCheck = benthossvc.HealthCheck{
		IsLive:  true,
		IsReady: true,
	}

	// Add these lines to simulate stability checks
	mockService.ServiceStates[testID].S6ObservedState.ServiceInfo.Uptime = 15
	mockService.ServiceStates[testID].BenthosStatus.MetricsState = &benthossvc.BenthosMetricsState{
		IsActive: false,
	}

	if err, _ := instance.Reconcile(ctx, tick); err != nil {
		return tick, err
	}
	tick++

	// 7. Final transition to Idle
	// Let's simulate 3 successful reconciliations (to test the stability check)
	for i := 0; i < 3; i++ {
		if err, _ := instance.Reconcile(ctx, tick); err != nil {
			return tick, err
		}
		tick++
	}

	return tick, nil
}

var _ = Describe("Benthos FSM", func() {
	var (
		mockService *benthossvc.MockBenthosService
		instance    *BenthosInstance
		testID      string
		ctx         context.Context
		tick        uint64
	)

	BeforeEach(func() {
		testID = "test-benthos"
		mockService = benthossvc.NewMockBenthosService()
		instance = createMockBenthosInstance(testID, mockService)
		ctx = context.Background()
		tick = 0
	})

	Context("Basic State Transitions", func() {
		It("should transition from Stopped to Starting when activated", func() {
			// First go through the lifecycle states
			// Initial state should be to_be_created
			Expect(instance.GetCurrentFSMState()).To(Equal(internalfsm.LifecycleStateToBeCreated))

			// Reconcile should trigger create event
			err, _ := instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(internalfsm.LifecycleStateCreating))
			Expect(mockService.AddBenthosToS6ManagerCalled).To(BeTrue())

			// Mock service creation success
			mockService.ServiceStates[testID] = &benthossvc.ServiceInfo{
				BenthosStatus: benthossvc.BenthosStatus{
					HealthCheck: benthossvc.HealthCheck{
						IsLive:  false,
						IsReady: false,
					},
				},
				S6FSMState: s6fsm.OperationalStateStopped,
			}

			// Another reconcile should complete the creation
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped))

			// Now test the operational state transition
			// Set desired state to active
			Expect(instance.SetDesiredFSMState(OperationalStateActive)).To(Succeed())
			Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateActive))

			// Setup mock service state for initial reconciliation
			mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
				IsS6Running: false,
			})

			// First reconciliation should trigger transition to Starting
			err, reconciled := instance.Reconcile(ctx, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciled).To(BeTrue())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting))

			// Verify service interactions
			Expect(mockService.StartBenthosCalled).To(BeTrue())
		})

		It("should transition from Starting to ConfigLoading when S6 is running", func() {
			// First complete the creation process
			// Initial state should be to_be_created
			Expect(instance.GetCurrentFSMState()).To(Equal(internalfsm.LifecycleStateToBeCreated))

			// First reconcile moves to creating
			err, _ := instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(internalfsm.LifecycleStateCreating))
			Expect(mockService.AddBenthosToS6ManagerCalled).To(BeTrue())

			// Mock service creation success
			mockService.ServiceStates[testID] = &benthossvc.ServiceInfo{
				BenthosStatus: benthossvc.BenthosStatus{
					HealthCheck: benthossvc.HealthCheck{
						IsLive:  false,
						IsReady: false,
					},
				},
				S6FSMState: s6fsm.OperationalStateStopped,
			}

			// Second reconcile completes creation to stopped
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped))

			// Activate the instance
			Expect(instance.SetDesiredFSMState(OperationalStateActive)).To(Succeed())

			// First operational reconcile moves to starting
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting))

			// Now setup S6 running state and mock config response
			mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
				IsS6Running:          true,
				S6FSMState:           s6fsm.OperationalStateRunning,
				IsConfigLoaded:       true,
				IsHealthchecksPassed: true,
			})

			// Update health check status
			mockService.ServiceStates[testID].BenthosStatus.HealthCheck = benthossvc.HealthCheck{
				IsLive:  true,
				IsReady: true,
			}

			// Reconcile again - should transition to ConfigLoading
			err, reconciled := instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciled).To(BeTrue())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStartingConfigLoading))
		})

		It("should transition to Idle when healthchecks pass", func() {
			// First get to ConfigLoading state
			// Initial creation flow
			Expect(instance.GetCurrentFSMState()).To(Equal(internalfsm.LifecycleStateToBeCreated))

			// 1. Create the service
			err, _ := instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(internalfsm.LifecycleStateCreating))

			// Mock service creation success
			mockService.ServiceStates[testID] = &benthossvc.ServiceInfo{
				S6FSMState: s6fsm.OperationalStateStopped,
			}

			// 2. Complete creation
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped))

			// 3. Activate the instance
			Expect(instance.SetDesiredFSMState(OperationalStateActive)).To(Succeed())

			// 4. Transition to Starting
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting))

			// 5. Transition to ConfigLoading
			mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
				IsS6Running: true,
				S6FSMState:  s6fsm.OperationalStateRunning,
			})
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStartingConfigLoading))

			// Now proceed with the test
			// First set healthchecks to NOT passed
			mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
				IsS6Running:          true,
				S6FSMState:           s6fsm.OperationalStateRunning,
				IsConfigLoaded:       true,
				IsHealthchecksPassed: false,
			})

			// Add required S6 observed state for config loaded check
			mockService.ServiceStates[testID].S6ObservedState.ServiceInfo = s6svc.ServiceInfo{
				Uptime: 5, // This enables IsBenthosConfigLoaded() check
			}

			// Update health check status to not ready
			mockService.ServiceStates[testID].BenthosStatus.HealthCheck = benthossvc.HealthCheck{
				IsLive:  true,
				IsReady: false,
			}

			// First reconcile should transition to WaitingForHealthchecks
			err, reconciled := instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciled).To(BeTrue())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStartingWaitingForHealthchecks))

			// Now set healthchecks to passed
			mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
				IsS6Running:          true,
				S6FSMState:           s6fsm.OperationalStateRunning,
				IsConfigLoaded:       true,
				IsHealthchecksPassed: true,
			})

			// Update health check status to ready
			mockService.ServiceStates[testID].BenthosStatus.HealthCheck = benthossvc.HealthCheck{
				IsLive:  true,
				IsReady: true,
			}

			// Final reconcile should transition out of WaitingForHealthchecks
			err, reconciled = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciled).To(BeTrue())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStartingWaitingForServiceToRemainRunning))

			// Add uptime simulation for stability checks
			mockService.ServiceStates[testID].S6ObservedState.ServiceInfo.Uptime = 15 // Simulate 15 seconds uptime

			// Transition to Idle
			err, reconciled = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciled).To(BeTrue())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateIdle))

			// Stability check period
			for i := 0; i < 3; i++ {
				err, reconciled = instance.Reconcile(ctx, tick)
				tick++
				Expect(err).NotTo(HaveOccurred())
				Expect(reconciled).To(BeFalse())
			}

			// Final check with sufficient uptime
			mockService.ServiceStates[testID].S6ObservedState.ServiceInfo.Uptime = 25
			err, reconciled = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciled).To(BeFalse())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateIdle))
		})
	})

	Context("Running State Transitions", func() {
		It("should transition from Idle to Active when processing data", func() {
			// First reach Idle state through normal startup process
			tick, err := getInstanceInIdleState(testID, mockService, instance, ctx, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateIdle))

			// Maintain healthy state flags
			mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
				IsS6Running:            true,
				IsConfigLoaded:         true,
				IsHealthchecksPassed:   true,
				IsRunningWithoutErrors: true,
				HasProcessingActivity:  true,
			})

			// Add this to initialize the MetricsState
			mockService.ServiceStates[testID].BenthosStatus.MetricsState = &benthossvc.BenthosMetricsState{
				IsActive: true, // This is what HasProcessingActivity checks
			}

			err, reconciled := instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciled).To(BeTrue())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateActive))
		})

		It("should transition to Degraded when issues occur", func() {
			// First reach Active state through normal startup and processing activity
			tick, err := getInstanceInIdleState(testID, mockService, instance, ctx, tick)
			Expect(err).NotTo(HaveOccurred())

			// Transition to Active state first
			mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
				IsS6Running:            true,
				IsConfigLoaded:         true,
				IsHealthchecksPassed:   true,
				IsRunningWithoutErrors: true,
				HasProcessingActivity:  true,
			})
			mockService.ServiceStates[testID].BenthosStatus.MetricsState = &benthossvc.BenthosMetricsState{
				IsActive: true,
			}
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateActive))

			// Simulate healthcheck failure
			mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
				IsS6Running:            true,
				IsConfigLoaded:         true,
				IsHealthchecksPassed:   false, // Healthcheck failure
				IsRunningWithoutErrors: true,
				HasProcessingActivity:  true,
			})
			mockService.ServiceStates[testID].BenthosStatus.HealthCheck = benthossvc.HealthCheck{
				IsLive:  false,
				IsReady: false,
			}

			// Trigger reconcile to detect degradation
			err, reconciled := instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciled).To(BeTrue())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateDegraded))

			// Verify metrics state is still present
			Expect(mockService.ServiceStates[testID].BenthosStatus.MetricsState).ToNot(BeNil())
			Expect(mockService.ServiceStates[testID].BenthosStatus.MetricsState.IsActive).To(BeTrue())
		})

		It("should recover from Degraded state when issues resolve", func() {
			// First reach Degraded state
			tick, err := getInstanceInIdleState(testID, mockService, instance, ctx, tick)
			Expect(err).NotTo(HaveOccurred())

			// Transition to Active then Degraded
			mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
				IsS6Running:            true,
				IsConfigLoaded:         true,
				IsHealthchecksPassed:   false,
				IsRunningWithoutErrors: true,
				HasProcessingActivity:  true,
			})
			mockService.ServiceStates[testID].BenthosStatus.HealthCheck = benthossvc.HealthCheck{
				IsLive:  false,
				IsReady: false,
			}
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateDegraded))

			// Fix the healthcheck issues
			mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
				IsS6Running:            true,
				IsConfigLoaded:         true,
				IsHealthchecksPassed:   true,
				IsRunningWithoutErrors: true,
				HasProcessingActivity:  true,
			})
			mockService.ServiceStates[testID].BenthosStatus.HealthCheck = benthossvc.HealthCheck{
				IsLive:  true,
				IsReady: true,
			}

			// Add stability checks (uptime > 10s)
			mockService.ServiceStates[testID].S6ObservedState.ServiceInfo.Uptime = 15

			// Trigger recovery
			err, reconciled := instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciled).To(BeTrue())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateIdle))

			// Verify stable state after recovery
			// To bring this to active, we would need to also have active metrics
			for i := 0; i < 3; i++ {
				err, reconciled = instance.Reconcile(ctx, tick)
				tick++
				Expect(err).NotTo(HaveOccurred())
				Expect(reconciled).To(BeFalse())
				Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateIdle))
			}

			// Now set the metrics to active
			mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
				IsS6Running:            true,
				IsConfigLoaded:         true,
				IsHealthchecksPassed:   true,
				IsRunningWithoutErrors: true,
				HasProcessingActivity:  true,
			})
			mockService.ServiceStates[testID].BenthosStatus.MetricsState = &benthossvc.BenthosMetricsState{
				IsActive: true,
			}

			err, reconciled = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciled).To(BeTrue())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateActive))
		})
	})

	Context("Stopping Flow", func() {
		// TODO: Test stopping from Active state
		It("should stop gracefully from Active state", func() {
			// To be implemented
		})

		// TODO: Test stopping from Degraded state
		It("should stop gracefully from Degraded state", func() {
			// To be implemented
		})
	})

	Context("complex state transitions", func() {

		It("should restart from Starting when config loading fails due to S6 instability", func() {
			// Reach ConfigLoading state first
			// 1. Create the service
			err, _ := instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(internalfsm.LifecycleStateCreating))

			// Mock service creation success
			mockService.ServiceStates[testID] = &benthossvc.ServiceInfo{
				S6FSMState: s6fsm.OperationalStateStopped,
			}

			// 2. Complete creation
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped))

			// 3. Activate the instance
			Expect(instance.SetDesiredFSMState(OperationalStateActive)).To(Succeed())

			// 4. Transition to Starting
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting))

			// 5. Transition to ConfigLoading
			mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
				IsS6Running: true,
				S6FSMState:  s6fsm.OperationalStateRunning,
			})
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStartingConfigLoading))

			// Verify we're in ConfigLoading state
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStartingConfigLoading))

			// Simulate S6 crash during config loading
			mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
				IsS6Running:          false,
				S6FSMState:           s6fsm.OperationalStateStopped,
				IsConfigLoaded:       false,
				IsHealthchecksPassed: false,
			})

			// First reconcile should detect failure and transition back to Starting
			err, reconciled := instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciled).To(BeTrue())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting))

			// Now simulate S6 comes back up but fails again before config loads
			mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
				IsS6Running:          true,
				S6FSMState:           s6fsm.OperationalStateRunning,
				IsConfigLoaded:       false,
				IsHealthchecksPassed: false,
			})

			// Reconcile should move back to ConfigLoading
			err, reconciled = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciled).To(BeTrue())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStartingConfigLoading))

			// Simulate another S6 crash
			mockService.SetServiceState(testID, benthossvc.ServiceStateFlags{
				IsS6Running:          false,
				S6FSMState:           s6fsm.OperationalStateStopped,
				IsConfigLoaded:       false,
				IsHealthchecksPassed: false,
			})

			// Final reconcile should return to Starting again
			err, reconciled = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciled).To(BeTrue())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting))

		})
	})

	Context("Error Handling", func() {
		// TODO: Test startup error handling
		It("should handle errors during startup", func() {
			// To be implemented
		})

		// TODO: Test runtime error handling
		It("should handle errors during runtime", func() {
			// To be implemented
		})
	})
})
