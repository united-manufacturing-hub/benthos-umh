package s6

import (
	"context"
	"fmt"
	"path/filepath"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/s6"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("S6 FSM", func() {
	var (
		mockService     *s6service.MockService
		instance        *S6Instance
		testID          string
		testS6BaseDir   string
		testServicePath string
		ctx             context.Context
	)

	BeforeEach(func() {
		testID = "test-service"
		testS6BaseDir = "/run/service"
		testServicePath = filepath.Join(testS6BaseDir, testID)
		mockService = s6service.NewMockService()
		mockService.ExistingServices[testS6BaseDir] = true

		var err error
		instance, err = NewS6InstanceWithService(testS6BaseDir, config.S6FSMConfig{
			FSMInstanceConfig: config.FSMInstanceConfig{
				Name: testID,
			},
		}, mockService)
		Expect(err).NotTo(HaveOccurred())
		ctx = context.Background()
	})

	It("should transition from to_be_created to stopped", func() {
		// Initial state should be to_be_created
		Expect(instance.GetCurrentFSMState()).To(Equal(internal_fsm.LifecycleStateToBeCreated))

		// Reconcile should trigger create event
		err, _ := instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(internal_fsm.LifecycleStateCreating))
		Expect(mockService.CreateCalled).To(BeTrue())

		// Mock service creation success
		mockService.ServiceStates[testServicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}

		// Another reconcile should complete the creation
		err, _ = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped))
	})

	It("should start the service when desired state is 'running'", func() {
		// Set up the instance in the stopped state
		instance.baseFSMInstance.SetCurrentFSMState(OperationalStateStopped)

		// Set service as existing
		mockService.ExistingServices[testServicePath] = true
		mockService.ServiceStates[testServicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}

		// Set desired state to running
		err := instance.SetDesiredFSMState(OperationalStateRunning)
		Expect(err).NotTo(HaveOccurred())

		// Reconcile should trigger start
		err, _ = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting))
		Expect(mockService.StartCalled).To(BeTrue())

		// Mock the service as running
		mockService.ServiceStates[testServicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceUp,
			Uptime: 10,
			Pid:    1234,
		}

		// Another reconcile should complete the start
		err, _ = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateRunning))
		Expect(instance.ObservedState.ServiceInfo.Status).To(Equal(s6service.ServiceUp))
		Expect(instance.GetServicePid()).To(Equal(1234))
	})

	It("should stop the service when desired state is 'stopped'", func() {
		// Set up the instance in the running state
		instance.baseFSMInstance.SetCurrentFSMState(OperationalStateRunning)

		// Set service as existing and running
		mockService.ExistingServices[testServicePath] = true
		mockService.ServiceStates[testServicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceUp,
			Uptime: 60,
			Pid:    1234,
		}

		// Set desired state to stopped
		err := instance.SetDesiredFSMState(OperationalStateStopped)
		Expect(err).NotTo(HaveOccurred())

		// Reconcile should trigger stop
		err, _ = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopping))
		Expect(mockService.StopCalled).To(BeTrue())

		// Mock the service as stopped
		mockService.ServiceStates[testServicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}

		// Another reconcile should complete the stop
		err, _ = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped))
		Expect(instance.ObservedState.ServiceInfo.Status).To(Equal(s6service.ServiceDown))
	})

	It("should handle errors during service operations", func() {
		// Set up the instance in the stopped state
		instance.baseFSMInstance.SetCurrentFSMState(OperationalStateStopped)

		// Set service as existing
		mockService.ExistingServices[testServicePath] = true
		mockService.ServiceStates[testServicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}

		// Set desired state to running
		err := instance.SetDesiredFSMState(OperationalStateRunning)
		Expect(err).NotTo(HaveOccurred())

		// Set up an error for the start operation
		mockService.StartError = fmt.Errorf("failed to start service")

		// Reconcile should attempt to start but encounter an error
		err, _ = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStopped)) // nothing changed
		Expect(mockService.StartCalled).To(BeTrue())

		// Check that the instance has recorded the error
		Expect(instance.baseFSMInstance.GetError()).NotTo(BeNil())
		Expect(instance.baseFSMInstance.GetError().Error()).To(ContainSubstring("failed to start service"))

		// Test that backoff prevents immediate retry
		mockService.StartCalled = false // Reset flag
		err, _ = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(mockService.StartCalled).To(BeFalse()) // Start shouldn't be called again due to backoff

		// Simulate backoff elapsing
		instance.baseFSMInstance.ResetState()
		mockService.StartError = nil

		// Mock the service as stopped
		mockService.ServiceStates[testServicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown, // Start with Down status
		}

		// First reconcile should trigger the start
		err, _ = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateStarting))
		Expect(mockService.StartCalled).To(BeTrue())

		// Now simulate the service starting up
		mockService.ServiceStates[testServicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceUp,
			Uptime: 10,
			Pid:    1234,
		}

		// Final reconcile to reach running state
		err, _ = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetCurrentFSMState()).To(Equal(OperationalStateRunning))

	})
})
