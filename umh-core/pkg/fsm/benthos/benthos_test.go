package benthos

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/benthos"
)

// createMockBenthosInstance creates a BenthosInstance with a mock service for testing
func createMockBenthosInstance(name string, mockService *benthos.MockBenthosService) *BenthosInstance {
	cfg := config.BenthosConfig{
		Name:                 name,
		BenthosServiceConfig: config.BenthosServiceConfig{},
	}

	instance := NewBenthosInstance("/run/service", cfg)
	instance.service = mockService
	return instance
}

var _ = Describe("Benthos FSM", func() {
	var (
		mockService *benthos.MockBenthosService
		instance    *BenthosInstance
		testID      string
		ctx         context.Context
		tick        uint64
	)

	BeforeEach(func() {
		testID = "test-benthos"
		mockService = benthos.NewMockBenthosService()
		instance = createMockBenthosInstance(testID, mockService)
		ctx = context.Background()
		tick = 0
	})

	Context("State Transitions", func() {
		It("should transition through startup states correctly", func() {
			// Initial state should be stopped
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped))

			// Set desired state to active
			err := instance.SetDesiredFSMState(OperationalStateActive)
			Expect(err).NotTo(HaveOccurred())

			// First reconcile should trigger start
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting))
			Expect(mockService.StartBenthosCalled).To(BeTrue())

			// Mock service as started
			mockService.ServiceStates[testID] = benthos.ServiceInfo{
				Status: benthos.ServiceUp,
			}

			// Next reconcile should move to config loading
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStartingConfigLoading))

			// Next reconcile should move to waiting for healthchecks
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStartingWaitingForHealthchecks))

			// Next reconcile should move to waiting for service to remain running
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStartingWaitingForServiceToRemainRunning))

			// Final reconcile should reach idle state
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateIdle))
		})

		It("should transition through running states correctly", func() {
			// Set up instance in idle state
			instance.baseFSMInstance.SetCurrentFSMState(OperationalStateIdle)
			mockService.ServiceStates[testID] = benthos.ServiceInfo{
				Status: benthos.ServiceUp,
			}

			// Simulate data processing
			instance.ObservedState.ServiceInfo.Status = benthos.ServiceUp
			err, _ := instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateActive))

			// Simulate warning
			instance.ObservedState.ServiceInfo.Status = benthos.ServiceWarning
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateDegraded))

			// Simulate recovery
			instance.ObservedState.ServiceInfo.Status = benthos.ServiceUp
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateIdle))
		})

		It("should handle shutdown correctly", func() {
			// Set up instance in active state
			instance.baseFSMInstance.SetCurrentFSMState(OperationalStateActive)
			mockService.ServiceStates[testID] = benthos.ServiceInfo{
				Status: benthos.ServiceUp,
			}

			// Set desired state to stopped
			err := instance.SetDesiredFSMState(OperationalStateStopped)
			Expect(err).NotTo(HaveOccurred())

			// First reconcile should trigger stop
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopping))
			Expect(mockService.StopBenthosCalled).To(BeTrue())

			// Mock service as stopped
			mockService.ServiceStates[testID] = benthos.ServiceInfo{
				Status: benthos.ServiceDown,
			}

			// Final reconcile should reach stopped state
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped))
		})
	})

	Context("Error Handling", func() {
		It("should handle start failures", func() {
			// Set desired state to active
			err := instance.SetDesiredFSMState(OperationalStateActive)
			Expect(err).NotTo(HaveOccurred())

			// Set up an error for the start operation
			mockService.StartBenthosError = benthos.ErrServiceNotExist

			// Reconcile should attempt to start but encounter an error
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped))
			Expect(mockService.StartBenthosCalled).To(BeTrue())

			// Check that the instance has recorded the error
			Expect(instance.baseFSMInstance.GetError()).NotTo(BeNil())
			Expect(instance.baseFSMInstance.GetError().Error()).To(ContainSubstring("service does not exist"))
		})

		It("should handle stop failures", func() {
			// Set up instance in active state
			instance.baseFSMInstance.SetCurrentFSMState(OperationalStateActive)
			mockService.ServiceStates[testID] = benthos.ServiceInfo{
				Status: benthos.ServiceUp,
			}

			// Set desired state to stopped
			err := instance.SetDesiredFSMState(OperationalStateStopped)
			Expect(err).NotTo(HaveOccurred())

			// Set up an error for the stop operation
			mockService.StopBenthosError = benthos.ErrServiceNotExist

			// Reconcile should attempt to stop but encounter an error
			err, _ = instance.Reconcile(ctx, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateActive))
			Expect(mockService.StopBenthosCalled).To(BeTrue())

			// Check that the instance has recorded the error
			Expect(instance.baseFSMInstance.GetError()).NotTo(BeNil())
			Expect(instance.baseFSMInstance.GetError().Error()).To(ContainSubstring("service does not exist"))
		})
	})

	Context("Configuration Validation", func() {
		It("should validate desired state changes", func() {
			// Test invalid desired state
			err := instance.SetDesiredFSMState("invalid_state")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid desired state"))

			// Test valid desired states
			err = instance.SetDesiredFSMState(OperationalStateStopped)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateStopped))

			err = instance.SetDesiredFSMState(OperationalStateActive)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetDesiredFSMState()).To(Equal(OperationalStateActive))
		})
	})

	Context("Service Lifecycle", func() {
		It("should handle service removal", func() {
			// Set up instance in stopped state
			instance.baseFSMInstance.SetCurrentFSMState(OperationalStateStopped)

			// Initiate removal
			err := instance.Remove(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Verify instance is marked for removal
			Expect(instance.IsRemoved()).To(BeTrue())
		})
	})
})
