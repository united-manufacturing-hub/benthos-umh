package s6

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// mockS6Instance is a helper type for testing that embeds S6Instance and allows mocking functions
type mockS6Instance struct {
	*S6Instance
	mockIsRunning bool
	mockIsStopped bool
	mockHasFailed bool
}

// Override IsS6Running for tests
func (m *mockS6Instance) IsS6Running() bool {
	status, _ := m.getS6Status(context.Background())
	return status == string(S6ServiceUp)
}

// Override IsS6Stopped for tests
func (m *mockS6Instance) IsS6Stopped() bool {
	status, _ := m.getS6Status(context.Background())
	return status == string(S6ServiceDown)
}

// Override HasS6Failed for tests
func (m *mockS6Instance) HasS6Failed() bool {
	status, _ := m.getS6Status(context.Background())
	return status == string(S6ServiceFailed)
}

// Override getS6Status for tests
func (m *mockS6Instance) getS6Status(ctx context.Context) (string, error) {
	if m.mockIsRunning {
		return string(S6ServiceUp), nil
	}
	if m.mockIsStopped {
		return string(S6ServiceDown), nil
	}
	if m.mockHasFailed {
		return string(S6ServiceFailed), nil
	}
	return string(S6ServiceRestarting), nil
}

var _ = Describe("S6Instance", func() {
	var (
		instance *S6Instance
		mock     *mockS6Instance
		ctx      context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("NewS6Instance", func() {
		BeforeEach(func() {
			instance = NewS6Instance("test-service", "/etc/s6/test-service", nil)
		})

		It("should create a new instance with correct initial state", func() {
			Expect(instance.GetState()).To(Equal(StateStopped))
			Expect(instance.GetDesiredState()).To(Equal(StateStopped))
			Expect(instance.ID).To(Equal("test-service"))
			Expect(instance.ServicePath).To(Equal("/etc/s6/test-service"))
		})
	})

	Describe("State Transitions", func() {
		BeforeEach(func() {
			instance = NewS6Instance("test-service", "/etc/s6/test-service", nil)
			mock = &mockS6Instance{
				S6Instance:    instance,
				mockIsRunning: true,
				mockIsStopped: false,
			}
		})

		Context("when starting the service", func() {
			It("should transition through the correct states", func() {
				// Set desired state to running
				Expect(mock.SetDesiredState(StateRunning)).To(Succeed())

				// Set initial state to stopped
				mock.FSM.SetState(StateStopped)

				// Initiate start
				Expect(mock.InitiateS6Start(ctx)).To(Succeed())

				// Send start event
				Expect(mock.sendEvent(ctx, EventStart)).To(Succeed())
				Expect(mock.GetState()).To(Equal(StateStarting))

				// Send start_done event
				Expect(mock.sendEvent(ctx, EventStartDone)).To(Succeed())
				Expect(mock.GetState()).To(Equal(StateRunning))
			})
		})

		Context("when stopping the service", func() {
			BeforeEach(func() {
				mock.mockIsRunning = false
				mock.mockIsStopped = true
			})

			It("should transition through the correct states", func() {
				// Set desired state to stopped
				Expect(mock.SetDesiredState(StateStopped)).To(Succeed())

				// Send stop event
				Expect(mock.sendEvent(ctx, EventStop)).To(Succeed())
				Expect(mock.GetState()).To(Equal(StateStopping))

				// Send stop_done event
				Expect(mock.sendEvent(ctx, EventStopDone)).To(Succeed())
				Expect(mock.GetState()).To(Equal(StateStopped))
			})
		})
	})

	Describe("Failure Handling", func() {
		BeforeEach(func() {
			instance = NewS6Instance("test-service", "/etc/s6/test-service", nil)
			mock = &mockS6Instance{
				S6Instance:    instance,
				mockHasFailed: true,
			}
			mock.FSM.SetState(StateRunning)
		})

		It("should transition to failed state", func() {
			Expect(mock.sendEvent(ctx, EventFail)).To(Succeed())
			Expect(mock.GetState()).To(Equal(StateFailed))
		})
	})

	Describe("Invalid State Transitions", func() {
		BeforeEach(func() {
			instance = NewS6Instance("test-service", "/etc/s6/test-service", nil)
		})

		It("should reject invalid desired states", func() {
			err := instance.SetDesiredState(StateStarting)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid desired state"))
		})
	})
})
