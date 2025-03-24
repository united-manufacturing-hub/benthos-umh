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

	Context("Lifecycle Creation Flow", func() {
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

			// Verify it lands up in to_be_created state
			tick, err := fsmtest.StabilizeS6Instance(ctx, instance, internal_fsm.LifecycleStateToBeCreated, 3, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetError()).To(BeNil())

			// Configure mock service to fail creation
			mockService.CreateError = fmt.Errorf("simulated create failure")

			// Verify it stays in to_be_created state during failure
			tick, err = fsmtest.VerifyStableState(ctx, instance, internal_fsm.LifecycleStateToBeCreated, 3, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetError()).ToNot(BeNil())

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
		})

		It("should remain in starting state until service is up", func() {
			instance, mockService, _ := fsmtest.SetupS6Instance(testBaseDir, "test-slow-start", s6.OperationalStateStopped)

			// Set desired state to running and transition to starting
			err := instance.SetDesiredFSMState(s6.OperationalStateRunning)
			Expect(err).NotTo(HaveOccurred())

			// First transition to starting
			tick, err := fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateStopped,
				s6.OperationalStateStarting, 5, tick)
			Expect(err).NotTo(HaveOccurred())

			// Keep service in starting state and verify it stays there
			mockService.ServiceStates[instance.GetServicePath()] = s6service.ServiceInfo{
				Status: s6service.ServiceRestarting,
			}

			// Use VerifyStableState to actively check state stability
			tick, err = fsmtest.VerifyStableState(ctx, instance, s6.OperationalStateStarting, 3, tick)
			Expect(err).NotTo(HaveOccurred())

			// Allow service to complete startup
			// This will also reset the mock service
			tick, err = fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateStarting,
				s6.OperationalStateRunning, 5, tick)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle start failure and retry after backoff", func() {
			instance, mockService, _ := fsmtest.SetupS6Instance(testBaseDir, "test-start-failure", s6.OperationalStateStopped)

			// First ensure we're in the stopped state by completing the creation lifecycle
			tick, err := fsmtest.TestS6StateTransition(ctx, instance, internal_fsm.LifecycleStateToBeCreated,
				s6.OperationalStateStopped, 3, tick)
			Expect(err).NotTo(HaveOccurred())

			// Configure mock service to fail start BEFORE we trigger transitions
			mockService.StartError = fmt.Errorf("simulated start failure")
			mockService.StartCalled = false

			// Set desired state to running
			err = instance.SetDesiredFSMState(s6.OperationalStateRunning)
			Expect(err).NotTo(HaveOccurred())

			// Reconcile a few times to verify it stays in stopped state due to error
			tick, err = fsmtest.VerifyStableState(ctx, instance, s6.OperationalStateStopped, 3, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetError()).NotTo(BeNil())
			Expect(mockService.StartCalled).To(BeTrue())

			// Fix the mock service and test transition
			mockService.StartError = nil
			mockService.StartCalled = false

			// Now it should complete the full transition sequence
			tick, err = fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateStopped,
				s6.OperationalStateStarting, 5, tick)
			Expect(err).NotTo(HaveOccurred())

			tick, err = fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateStarting,
				s6.OperationalStateRunning, 5, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(mockService.StartCalled).To(BeTrue())
		})
	})

	Context("Stopping Flow", func() {
		It("should transition from running to stopped", func() {
			// Set up and test full transition from running to stopped
			instance, mockService, _ := fsmtest.SetupS6Instance(testBaseDir, "test-stopping", s6.OperationalStateRunning)

			// First ensure we're in the running state by completing the creation lifecycle
			tick, err := fsmtest.TestS6StateTransition(ctx, instance, internal_fsm.LifecycleStateToBeCreated,
				s6.OperationalStateRunning, 5, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(s6.OperationalStateRunning))

			// Test complete transition
			tick, err = fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateRunning,
				s6.OperationalStateStopped, 10, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(mockService.StopCalled).To(BeTrue())
		})

		It("should remain in stopping state until service is down", func() {
			instance, mockService, _ := fsmtest.SetupS6Instance(testBaseDir, "test-slow-stop", s6.OperationalStateRunning)

			// First ensure we're in the running state by completing the creation lifecycle
			tick, err := fsmtest.TestS6StateTransition(ctx, instance, internal_fsm.LifecycleStateToBeCreated,
				s6.OperationalStateRunning, 5, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(s6.OperationalStateRunning))

			// Set desired state to stopped and transition to stopping
			err = instance.SetDesiredFSMState(s6.OperationalStateStopped)
			Expect(err).NotTo(HaveOccurred())

			// First transition to stopping
			tick, err = fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateRunning,
				s6.OperationalStateStopping, 5, tick)
			Expect(err).NotTo(HaveOccurred())

			// Keep service in a not-quite-stopped state and verify it stays in stopping
			mockService.ServiceStates[instance.GetServicePath()] = s6service.ServiceInfo{
				Status: s6service.ServiceRestarting,
			}

			// Use VerifyStableState to actively check state stability
			tick, err = fsmtest.VerifyStableState(ctx, instance, s6.OperationalStateStopping, 3, tick)
			Expect(err).NotTo(HaveOccurred())

			// Allow service to complete shutdown
			tick, err = fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateStopping,
				s6.OperationalStateStopped, 5, tick)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle stop failure and retry after backoff", func() {
			instance, mockService, _ := fsmtest.SetupS6Instance(testBaseDir, "test-stop-failure", s6.OperationalStateRunning)

			// First ensure we're in the running state by completing the creation lifecycle
			tick, err := fsmtest.TestS6StateTransition(ctx, instance, internal_fsm.LifecycleStateToBeCreated,
				s6.OperationalStateRunning, 5, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(s6.OperationalStateRunning))

			// Configure mock service to fail stop BEFORE triggering transitions
			mockService.StopError = fmt.Errorf("simulated stop failure")
			mockService.StopCalled = false

			// Set desired state to stopped
			err = instance.SetDesiredFSMState(s6.OperationalStateStopped)
			Expect(err).NotTo(HaveOccurred())

			// First verify it stays in running state due to stop error
			tick, err = fsmtest.VerifyStableState(ctx, instance, s6.OperationalStateRunning, 3, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetError()).NotTo(BeNil())
			Expect(mockService.StopCalled).To(BeTrue())

			// Fix the initial error to allow transition to stopping
			mockService.StopError = nil
			mockService.StopCalled = false

			// Now it should transition to stopping
			tick, err = fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateRunning,
				s6.OperationalStateStopping, 5, tick)
			Expect(err).NotTo(HaveOccurred())

			// Configure another error for the stopping state
			// Service remains running
			mockService.ServiceStates[instance.GetServicePath()] = s6service.ServiceInfo{
				Status: s6service.ServiceUp,
			}

			// Verify it stays in stopping state during failure
			tick, err = fsmtest.VerifyStableState(ctx, instance, s6.OperationalStateStopping, 3, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetError()).To(BeNil())
			Expect(mockService.StopCalled).To(BeTrue())

			// Allow service to complete stopping
			tick, err = fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateStopping,
				s6.OperationalStateStopped, 5, tick)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	FContext("Error Handling and Backoff", func() {
		It("should apply backoff when operations fail repeatedly", func() {
			instance, mockService, _ := fsmtest.SetupS6Instance(testBaseDir, "test-backoff", s6.OperationalStateStopped)

			// First ensure we're in the stopped state by completing the creation lifecycle
			tick, err := fsmtest.TestS6StateTransition(ctx, instance, internal_fsm.LifecycleStateToBeCreated,
				s6.OperationalStateStopped, 5, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(s6.OperationalStateStopped))

			// Configure mock service to fail
			mockService.StartError = fmt.Errorf("simulated start failure")
			mockService.StartCalled = false

			// Set desired state to running
			err = instance.SetDesiredFSMState(s6.OperationalStateRunning)
			Expect(err).NotTo(HaveOccurred())

			// Verify the instance stays in stopped state due to error (with multiple reconciliations)
			tick, err = fsmtest.VerifyStableState(ctx, instance, s6.OperationalStateStopped, 5, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(mockService.StartCalled).To(BeTrue())
			Expect(instance.GetError()).NotTo(BeNil())

			// After error occurs, reset flag to check if backoff is active
			mockService.StartCalled = false

			// Verify backoff is active (service start should not be called again)
			tick, err = fsmtest.VerifyStableState(ctx, instance, s6.OperationalStateStopped, 1, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(mockService.StartCalled).To(BeFalse()) // Backoff should prevent this
			Expect(instance.GetError()).NotTo(BeNil())
		})

		It("should reset backoff after successful operation", func() {
			instance, mockService, _ := fsmtest.SetupS6Instance(testBaseDir, "test-backoff-reset", s6.OperationalStateStopped)

			// First ensure we're in the stopped state by completing the creation lifecycle
			tick, err := fsmtest.TestS6StateTransition(ctx, instance, internal_fsm.LifecycleStateToBeCreated,
				s6.OperationalStateStopped, 5, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetCurrentFSMState()).To(Equal(s6.OperationalStateStopped))

			// Configure service state to ensure it's properly stopped
			mockService.ServiceStates[instance.GetServicePath()] = s6service.ServiceInfo{
				Status: s6service.ServiceDown,
			}

			// Configure mock service to fail start
			mockService.StartError = fmt.Errorf("simulated start failure")
			mockService.StartCalled = false

			// Set desired state to running
			err = instance.SetDesiredFSMState(s6.OperationalStateRunning)
			Expect(err).NotTo(HaveOccurred())

			// Verify it stays in stopped state due to errors (causing backoff to activate)
			tick, err = fsmtest.VerifyStableState(ctx, instance, s6.OperationalStateStopped, 5, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetError()).NotTo(BeNil())
			Expect(mockService.StartCalled).To(BeTrue())

			// Reset flag to check if backoff is active
			mockService.StartCalled = false

			// Verify backoff is active
			tick, err = fsmtest.VerifyStableState(ctx, instance, s6.OperationalStateStopped, 1, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(mockService.StartCalled).To(BeFalse())

			// Fix the error and clear instance error state
			mockService.StartError = nil
			fsmtest.ResetInstanceError(instance)
			mockService.StartCalled = false

			// Now it should transition to starting state
			tick, err = fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateStopped,
				s6.OperationalStateStarting, 5, tick)
			Expect(err).NotTo(HaveOccurred())
			Expect(mockService.StartCalled).To(BeTrue())

			// Configure service as running
			mockService.ServiceStates[instance.GetServicePath()] = s6service.ServiceInfo{
				Status: s6service.ServiceUp,
				Pid:    1234,
				Uptime: 10,
			}

			// Complete transition to running
			tick, err = fsmtest.TestS6StateTransition(ctx, instance, s6.OperationalStateStarting,
				s6.OperationalStateRunning, 5, tick)
			Expect(err).NotTo(HaveOccurred())

			// Now try causing a failure in the running state (e.g., health check)
			// This shows backoff was reset after successful start
			// TODO: Implement this after adding health check failure tests
		})
	})
})
