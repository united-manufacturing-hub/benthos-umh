package benthos

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	internalfsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	benthossvc "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/benthos"
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
			mockService.ServiceStates[testID] = benthossvc.ServiceInfo{
				BenthosStatus: benthossvc.BenthosStatus{
					HealthCheck: benthossvc.HealthCheck{
						IsLive:  false,
						IsReady: false,
					},
				},
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

		// TODO: Test transition from Starting to ConfigLoading
		It("should transition from Starting to ConfigLoading when S6 is running", func() {
			// To be implemented
		})

		// TODO: Test transition to WaitingForHealthchecks
		It("should transition to WaitingForHealthchecks when config is loaded", func() {
			// To be implemented
		})
	})

	Context("Running State Transitions", func() {
		// TODO: Test Idle -> Active transition
		It("should transition from Idle to Active when processing data", func() {
			// To be implemented
		})

		// TODO: Test Active -> Degraded transition
		It("should transition to Degraded when issues occur", func() {
			// To be implemented
		})

		// TODO: Test recovery from Degraded state
		It("should recover from Degraded state when issues resolve", func() {
			// To be implemented
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
