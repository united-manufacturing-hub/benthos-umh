package s6

import (
	"context"
	"fmt"
	"testing"

	"github.com/looplab/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/fsm/utils"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/service/s6"

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

		// Create callbacks for testing state transitions
		callbacks := map[string]fsm.Callback{
			"enter_" + utils.LifecycleStateCreating: func(ctx context.Context, e *fsm.Event) {
				GinkgoWriter.Write([]byte("Entering creating state\n"))
			},
			"enter_" + OperationalStateStopped: func(ctx context.Context, e *fsm.Event) {
				GinkgoWriter.Write([]byte("Entering stopped state\n"))
			},
			"enter_" + OperationalStateStarting: func(ctx context.Context, e *fsm.Event) {
				GinkgoWriter.Write([]byte("Entering starting state\n"))
			},
			"enter_" + OperationalStateRunning: func(ctx context.Context, e *fsm.Event) {
				GinkgoWriter.Write([]byte("Entering running state\n"))
			},
		}

		instance = NewS6InstanceWithService(testID, testPath, mockService, s6service.ServiceConfig{}, callbacks)
		ctx = context.Background()
	})

	It("should transition from to_be_created to stopped", func() {
		// Initial state should be to_be_created
		Expect(instance.GetCurrentFSMState()).To(Equal(utils.LifecycleStateToBeCreated))

		// Reconcile should trigger create event
		err := instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(utils.LifecycleStateCreating))
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
		instance.fsm.SetState(OperationalStateStopped)

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
		instance.fsm.SetState(OperationalStateRunning)

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
		instance.fsm.SetState(OperationalStateStopped)

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
		Expect(instance.getError()).NotTo(BeNil())
		Expect(instance.getError().Error()).To(ContainSubstring("failed to start service"))

		// Test that backoff prevents immediate retry
		mockService.StartCalled = false // Reset flag
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(mockService.StartCalled).To(BeFalse()) // Start shouldn't be called again due to backoff

		// Simulate backoff elapsing
		instance.backoff.Reset()

		// Clear the error to allow success
		mockService.StartError = nil
		instance.clearError()

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
