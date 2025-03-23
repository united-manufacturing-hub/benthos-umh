package s6_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsmtest"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	s6fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm/s6"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/s6"
)

var _ = Describe("S6Manager", func() {
	var (
		manager *s6fsm.S6Manager
		ctx     context.Context
		tick    uint64
	)

	BeforeEach(func() {
		ctx = context.Background()
		manager = s6fsm.NewS6ManagerWithMockedServices("")
		tick = uint64(0)
	})

	Context("Initialization", func() {
		It("should handle empty config without errors", func() {
			// Create empty config
			emptyConfig := []config.S6FSMConfig{}

			// Reconcile with empty config using a single reconciliation
			err, _ := manager.Reconcile(ctx, config.FullConfig{Services: emptyConfig}, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())

			// Verify that no instances were created
			Expect(manager.GetInstances()).To(BeEmpty())
		})

		It("should initialize service in stopped state and maintain it", func() {
			// First setup with empty config to ensure no instances exist
			emptyConfig := []config.S6FSMConfig{}
			err, _ := manager.Reconcile(ctx, config.FullConfig{Services: emptyConfig}, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())
			Expect(manager.GetInstances()).To(BeEmpty())

			// Create a service name for testing
			serviceName := "test-stopped-service"

			// Create config with explicitly desired stopped state
			configWithStoppedService := []config.S6FSMConfig{
				{
					FSMInstanceConfig: config.FSMInstanceConfig{
						Name:            serviceName,
						DesiredFSMState: s6fsm.OperationalStateStopped,
					},
					S6ServiceConfig: config.S6ServiceConfig{
						Command:     []string{"/bin/sh", "-c", "echo hello"},
						Env:         map[string]string{"TEST_ENV": "test_value"},
						ConfigFiles: map[string]string{},
					},
				},
			}

			// Reconcile to create service and ensure it reaches stopped state
			var nextTick uint64
			nextTick, err = fsmtest.WaitForMockedManagerInstanceState(ctx, manager, config.FullConfig{Services: configWithStoppedService},
				serviceName, s6fsm.OperationalStateStopped, 10, tick)
			tick = nextTick
			Expect(err).NotTo(HaveOccurred())

			// Verify service properties
			Expect(manager.GetInstances()).To(HaveLen(1))
			Expect(manager.GetInstances()).To(HaveKey(serviceName))

			service, ok := manager.GetInstance(serviceName)
			Expect(ok).To(BeTrue())

			// Verify state is stopped
			Expect(service.GetCurrentFSMState()).To(Equal(s6fsm.OperationalStateStopped))
			Expect(service.GetDesiredFSMState()).To(Equal(s6fsm.OperationalStateStopped))

			// Verify it's using a mock service
			s6Instance, ok := service.(*s6fsm.S6Instance)
			Expect(ok).To(BeTrue())

			mockService, ok := s6Instance.GetService().(*s6service.MockService)
			Expect(ok).To(BeTrue())
			Expect(mockService).NotTo(BeNil())

			// Verify Start was not called (since we're only expecting stopped state)
			Expect(mockService.StartCalled).To(BeFalse())

			// Verify state remains stable over multiple reconciliation cycles
			nextTick, err = fsmtest.WaitForMockedManagerInstanceState(ctx, manager, config.FullConfig{Services: configWithStoppedService},
				serviceName, s6fsm.OperationalStateStopped, 3, tick)
			tick = nextTick
			Expect(err).NotTo(HaveOccurred())

			// Double-check the final state
			Expect(service.GetCurrentFSMState()).To(Equal(s6fsm.OperationalStateStopped))
			Expect(service.GetDesiredFSMState()).To(Equal(s6fsm.OperationalStateStopped))
		})
	})

	// Future tests for state transitions would go here
	// Context("State Transitions", func() {
	//    It("should transition service from stopped to running", func() {
	//       // Test state transition logic
	//    })
	// })
})
