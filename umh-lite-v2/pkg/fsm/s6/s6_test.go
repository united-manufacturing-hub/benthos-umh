package s6

import (
	"context"
	"fmt"
	"testing"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/fsm"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/pkg/service/s6"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestS6(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "S6 FSM Suite")
}

var _ = Describe("S6 FSM", func() {
	var (
		mockService *s6service.MockService
		instance    *S6Instance
		testID      string
		testPath    string
		ctx         context.Context
	)

	BeforeEach(func() {
		testID = "test-service"
		testPath = "/tmp/test-service"
		mockService = s6service.NewMockService()

		instance = NewS6InstanceWithService(testID, testPath, mockService, s6service.ServiceConfig{})
		ctx = context.Background()
	})

	It("should transition from to_be_created to stopped", func() {
		// Initial state should be to_be_created
		Expect(instance.GetCurrentFSMState()).To(Equal(internal_fsm.LifecycleStateToBeCreated))

		// Reconcile should trigger create event
		err := instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(internal_fsm.LifecycleStateCreating))
		Expect(mockService.CreateCalled).To(BeTrue())

		// Mock service creation success
		mockService.ServiceStates[testPath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}

		// Another reconcile should complete the creation
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped))
	})

	It("should start the service when desired state is 'running'", func() {
		// Set up the instance in the stopped state
		instance.baseFSMInstance.SetCurrentFSMState(OperationalStateStopped)

		// Set service as existing
		mockService.ExistingServices[testPath] = true
		mockService.ServiceStates[testPath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}

		// Set desired state to running
		err := instance.SetDesiredFSMState(OperationalStateRunning)
		Expect(err).NotTo(HaveOccurred())

		// Reconcile should trigger start
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting))
		Expect(mockService.StartCalled).To(BeTrue())

		// Mock the service as running
		mockService.ServiceStates[testPath] = s6service.ServiceInfo{
			Status: s6service.ServiceUp,
			Uptime: 10,
			Pid:    1234,
		}

		// Another reconcile should complete the start
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateRunning))
		Expect(instance.ObservedState.Status).To(Equal(S6ServiceUp))
		Expect(instance.GetServicePid()).To(Equal(1234))
	})

	It("should stop the service when desired state is 'stopped'", func() {
		// Set up the instance in the running state
		instance.baseFSMInstance.SetCurrentFSMState(OperationalStateRunning)

		// Set service as existing and running
		mockService.ExistingServices[testPath] = true
		mockService.ServiceStates[testPath] = s6service.ServiceInfo{
			Status: s6service.ServiceUp,
			Uptime: 60,
			Pid:    1234,
		}

		// Set desired state to stopped
		err := instance.SetDesiredFSMState(OperationalStateStopped)
		Expect(err).NotTo(HaveOccurred())

		// Reconcile should trigger stop
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopping))
		Expect(mockService.StopCalled).To(BeTrue())

		// Mock the service as stopped
		mockService.ServiceStates[testPath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}

		// Another reconcile should complete the stop
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped))
		Expect(instance.ObservedState.Status).To(Equal(S6ServiceDown))
	})

	It("should handle errors during service operations", func() {
		// Set up the instance in the stopped state
		instance.baseFSMInstance.SetCurrentFSMState(OperationalStateStopped)

		// Set service as existing
		mockService.ExistingServices[testPath] = true
		mockService.ServiceStates[testPath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}

		// Set desired state to running
		err := instance.SetDesiredFSMState(OperationalStateRunning)
		Expect(err).NotTo(HaveOccurred())

		// Set up an error for the start operation
		mockService.StartError = fmt.Errorf("failed to start service")

		// Reconcile should attempt to start but encounter an error
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped)) // nothing changed
		Expect(mockService.StartCalled).To(BeTrue())

		// Check that the instance has recorded the error
		Expect(instance.baseFSMInstance.GetError()).NotTo(BeNil())
		Expect(instance.baseFSMInstance.GetError().Error()).To(ContainSubstring("failed to start service"))

		// Test that backoff prevents immediate retry
		mockService.StartCalled = false // Reset flag
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(mockService.StartCalled).To(BeFalse()) // Start shouldn't be called again due to backoff

		// Simulate backoff elapsing
		instance.baseFSMInstance.ResetState()
		mockService.StartError = nil

		// Mock the service as stopped
		mockService.ServiceStates[testPath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown, // Start with Down status
		}

		// First reconcile should trigger the start
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting))
		Expect(mockService.StartCalled).To(BeTrue())

		// Now simulate the service starting up
		mockService.ServiceStates[testPath] = s6service.ServiceInfo{
			Status: s6service.ServiceUp,
			Uptime: 10,
			Pid:    1234,
		}

		// Final reconcile to reach running state
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateRunning))

	})
})
