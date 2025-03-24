package s6_test

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsmtest"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/constants"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm/s6"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/s6"
)

var _ = Describe("S6 Instance", func() {
	var (
		ctx         context.Context
		testBaseDir string
		tick        uint64
	)

	BeforeEach(func() {
		ctx = context.Background()
		testBaseDir = constants.S6BaseDir
		tick = 0
	})

	FContext("Lifecycle Creation Flow", func() {
		It("should transition from to_be_created to stopped", func() {
			// Set up instance in to_be_created state
			instance, mockService, _ := fsmtest.SetupS6Instance(testBaseDir, "test-creation", s6.OperationalStateStopped)

			// Initial state should be to_be_created
			Expect(instance.GetCurrentFSMState()).To(Equal(internal_fsm.LifecycleStateToBeCreated))

			// Test transition through creating to stopped
			nextTick, err := fsmtest.TestS6StateTransition(ctx, instance, internal_fsm.LifecycleStateToBeCreated,
				internal_fsm.LifecycleStateCreating, 3, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(mockService.CreateCalled).To(BeTrue())

			// Then to stopped state
			_, err = fsmtest.TestS6StateTransition(ctx, instance, internal_fsm.LifecycleStateCreating,
				s6.OperationalStateStopped, 3, nextTick)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle creation failure and retry", func() {
			// Set up instance in to_be_created state
			instance, mockService, _ := fsmtest.SetupS6Instance(testBaseDir, "test-creation-failure", s6.OperationalStateStopped)

			// Verify it stays in to_be_created state during failure
			tick, err := fsmtest.StabilizeS6Instance(ctx, instance, internal_fsm.LifecycleStateToBeCreated, 3, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetError()).To(BeNil())

			// Configure mock service to fail creation
			mockService.CreateError = fmt.Errorf("simulated create failure")

			// Verify it stays in to_be_created state during failure
			tick, err = fsmtest.StabilizeS6Instance(ctx, instance, internal_fsm.LifecycleStateToBeCreated, 3, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetError()).To(BeNil())

			// Fix the mock service and test transition
			mockService.CreateError = nil
			mockService.CreateCalled = false

			// Test transition to creating and then stopped
			tick, err = fsmtest.TestS6StateTransition(ctx, instance, internal_fsm.LifecycleStateToBeCreated,
				internal_fsm.LifecycleStateCreating, 10, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(mockService.CreateCalled).To(BeTrue())

			_, err = fsmtest.TestS6StateTransition(ctx, instance, internal_fsm.LifecycleStateCreating,
				s6.OperationalStateStopped, 10, tick)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle permanent creation failure", func() {
			// TODO: Implement test for permanent creation failure
		})
	})

	Context("Starting Flow", func() {
		It("should transition from stopped to running", func() {
			// Set up and test full transition from stopped to running
			instance, mockService, _ := fsmtest.SetupS6Instance(testBaseDir, "test-starting", s6.OperationalStateStopped)

			// Test complete transition through all states
			_, err := fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateStopped,
				s6.OperationalStateRunning, 10, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(mockService.StartCalled).To(BeTrue())
			Expect(instance.GetServicePid()).To(Equal(1234))
		})

		It("should remain in starting state until service is up", func() {
			instance, mockService, _ := fsmtest.SetupS6Instance(testBaseDir, "test-slow-start", s6.OperationalStateStopped)

			// Set desired state to running and transition to starting
			err := instance.SetDesiredFSMState(s6.OperationalStateRunning)
			Expect(err).NotTo(HaveOccurred())

			// First transition to starting
			nextTick, err := fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateStopped,
				s6.OperationalStateStarting, 5, tick)
			Expect(err).NotTo(HaveOccurred())

			// Keep service in starting state and verify it stays there
			mockService.ServiceStates[instance.GetServicePath()] = s6service.ServiceInfo{
				Status: s6service.ServiceRestarting,
			}
			nextTick, err = fsmtest.StabilizeS6Instance(ctx, instance, s6.OperationalStateStarting, 3, nextTick)
			Expect(err).NotTo(HaveOccurred())

			// Allow service to complete startup
			_, err = fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateStarting,
				s6.OperationalStateRunning, 5, nextTick)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle start failure and retry after backoff", func() {
			// TODO: Implement test for start failure with backoff
		})
	})

	Context("Stopping Flow", func() {
		It("should transition from running to stopped", func() {
			// Set up and test full transition from running to stopped
			instance, mockService, _ := fsmtest.SetupS6Instance(testBaseDir, "test-stopping", s6.OperationalStateRunning)

			// Test complete transition
			_, err := fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateRunning,
				s6.OperationalStateStopped, 10, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(mockService.StopCalled).To(BeTrue())
		})

		It("should remain in stopping state until service is down", func() {
			instance, mockService, _ := fsmtest.SetupS6Instance(testBaseDir, "test-slow-stop", s6.OperationalStateRunning)

			// Set desired state to stopped and transition to stopping
			err := instance.SetDesiredFSMState(s6.OperationalStateStopped)
			Expect(err).NotTo(HaveOccurred())

			// First transition to stopping
			nextTick, err := fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateRunning,
				s6.OperationalStateStopping, 5, tick)
			Expect(err).NotTo(HaveOccurred())

			// Keep service in a not-quite-stopped state and verify it stays in stopping
			mockService.ServiceStates[instance.GetServicePath()] = s6service.ServiceInfo{
				Status: s6service.ServiceRestarting,
			}
			nextTick, err = fsmtest.StabilizeS6Instance(ctx, instance, s6.OperationalStateStopping, 3, nextTick)
			Expect(err).NotTo(HaveOccurred())

			// Allow service to complete shutdown
			_, err = fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateStopping,
				s6.OperationalStateStopped, 5, nextTick)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle stop failure and retry after backoff", func() {
			// TODO: Implement test for stop failure with backoff
		})
	})

	Context("Error Handling and Backoff", func() {
		It("should apply backoff when operations fail repeatedly", func() {
			// TODO: Implement test for backoff mechanism
		})

		It("should reset backoff after successful operation", func() {
			// TODO: Implement test for backoff reset
		})

		It("should handle maximum backoff reached", func() {
			// TODO: Implement test for max backoff
		})
	})
})
