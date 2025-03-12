package models_test

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/looplab/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/models"
)

var _ = Describe("BenthosInstance FSM", func() {
	var (
		instance       *models.BenthosInstance
		s6Service      *models.S6ServiceMock
		serviceManager *models.BenthosServiceManager
		callbacks      map[string]fsm.Callback
		ctx            context.Context
		cancelFunc     context.CancelFunc
	)

	BeforeEach(func() {
		ctx, cancelFunc = context.WithTimeout(context.Background(), 5*time.Second)
		callbacks = make(map[string]fsm.Callback)

		// Create a temporary directory for configs
		tempDir := GinkgoT().TempDir()

		var err error
		serviceManager, err = models.NewBenthosServiceManager(tempDir)
		Expect(err).NotTo(HaveOccurred())

		s6Service = models.NewS6ServiceMock("test-benthos")

		// Initialize callbacks for state transitions
		callbacks["enter_"+models.StateStarting] = func(ctx context.Context, e *fsm.Event) {
			GinkgoWriter.Printf("Starting service %s\n", e.FSM.Current())
		}

		callbacks["enter_"+models.StateRunning] = func(ctx context.Context, e *fsm.Event) {
			GinkgoWriter.Printf("Service now running %s\n", e.FSM.Current())
		}

		callbacks["enter_"+models.StateStopped] = func(ctx context.Context, e *fsm.Event) {
			GinkgoWriter.Printf("Service stopped %s\n", e.FSM.Current())
		}

		instance = models.NewBenthosInstance("test", callbacks)

		// Register service in manager
		serviceManager.RegisterService("test", s6Service)
	})

	AfterEach(func() {
		cancelFunc()
	})

	Describe("Initialization", func() {
		It("should start in the stopped state", func() {
			Expect(instance.GetState()).To(Equal(models.StateStopped))
		})

		It("should have desired state set to stopped", func() {
			Expect(instance.GetDesiredState()).To(Equal(models.StateStopped))
		})
	})

	Describe("State transitions", func() {
		It("should transition from stopped to starting to running", func() {
			// Set desired state to running
			instance.SetDesiredState(models.StateRunning)
			Expect(instance.GetDesiredState()).To(Equal(models.StateRunning))

			// Send start event
			err := instance.SendEvent(ctx, models.EventStart)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(models.StateStarting))

			// Simulate service starting
			err = serviceManager.Start(ctx, "test")
			Expect(err).NotTo(HaveOccurred())
			Expect(s6Service.IsRunning()).To(BeTrue())

			// Send started event
			err = instance.SendEvent(ctx, models.EventStarted)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(models.StateRunning))
		})

		It("should transition from running to stopped", func() {
			// First get to running state
			err := instance.SendEvent(ctx, models.EventStart)
			Expect(err).NotTo(HaveOccurred())
			err = serviceManager.Start(ctx, "test")
			Expect(err).NotTo(HaveOccurred())
			err = instance.SendEvent(ctx, models.EventStarted)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(models.StateRunning))

			// Now stop
			err = instance.SendEvent(ctx, models.EventStop)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(models.StateStopped))

			// Simulate service stopping
			err = serviceManager.Stop(ctx, "test")
			Expect(err).NotTo(HaveOccurred())
			Expect(s6Service.IsRunning()).To(BeFalse())
		})

		It("should handle config changes", func() {
			// First get to running state
			err := instance.SendEvent(ctx, models.EventStart)
			Expect(err).NotTo(HaveOccurred())
			err = serviceManager.Start(ctx, "test")
			Expect(err).NotTo(HaveOccurred())
			err = instance.SendEvent(ctx, models.EventStarted)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(models.StateRunning))

			// Set config and trigger config change
			instance.DesiredConfig = models.NewBenthosConfig("new: config")
			err = instance.SendEvent(ctx, models.EventConfigChange)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(models.StateConfigChanged))

			// Update config
			err = instance.SendEvent(ctx, models.EventUpdateConfig)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(models.StateChangingConfig))

			// Write and update service
			_, err = serviceManager.WriteConfig("test", "new: config")
			Expect(err).NotTo(HaveOccurred())
			err = serviceManager.Update(ctx, "test", "new: config")
			Expect(err).NotTo(HaveOccurred())

			// Done updating
			err = instance.SendEvent(ctx, models.EventUpdateDone)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(models.StateStarting))

			// Back to running
			err = instance.SendEvent(ctx, models.EventStarted)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(models.StateRunning))
		})

		It("should handle errors and recovery", func() {
			// First get to running state
			err := instance.SendEvent(ctx, models.EventStart)
			Expect(err).NotTo(HaveOccurred())

			// Simulate a failure during start
			s6Service.ShouldFailOnStart = true
			err = serviceManager.Start(ctx, "test")
			Expect(err).To(HaveOccurred())

			// Transition to error state
			err = instance.SendEvent(ctx, models.EventFail)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(models.StateError))

			// Record the error
			instance.SetError(errors.New("service failed to start"))
			Expect(instance.GetError()).To(MatchError("service failed to start"))

			// Allow retry
			s6Service.ShouldFailOnStart = false

			// Retry
			err = instance.SendEvent(ctx, models.EventRetry)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(models.StateStopped))

			// Clear error
			instance.ClearError()
			Expect(instance.GetError()).To(BeNil())

			// Start again successfully
			err = instance.SendEvent(ctx, models.EventStart)
			Expect(err).NotTo(HaveOccurred())
			err = serviceManager.Start(ctx, "test")
			Expect(err).NotTo(HaveOccurred())
			err = instance.SendEvent(ctx, models.EventStarted)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(models.StateRunning))
		})
	})

	Describe("S6 service interactions", func() {
		It("should interact properly with s6 service lifecycle", func() {
			// Service should start stopped
			Expect(s6Service.GetStatus()).To(Equal(models.S6ServiceDown))

			// Start service
			err := serviceManager.Start(ctx, "test")
			Expect(err).NotTo(HaveOccurred())
			Expect(s6Service.GetStatus()).To(Equal(models.S6ServiceUp))

			// Service should be running
			Expect(serviceManager.IsRunning("test")).To(BeTrue())

			// Stop service
			err = serviceManager.Stop(ctx, "test")
			Expect(err).NotTo(HaveOccurred())
			Expect(s6Service.GetStatus()).To(Equal(models.S6ServiceDown))

			// Service should be stopped
			Expect(serviceManager.IsRunning("test")).To(BeFalse())
		})

		It("should handle service failures", func() {
			// Set service to fail on start
			s6Service.ShouldFailOnStart = true

			// Try to start service, should fail
			err := serviceManager.Start(ctx, "test")
			Expect(err).To(HaveOccurred())
			Expect(s6Service.GetStatus()).To(Equal(models.S6ServiceFailed))

			// Reset service
			s6Service.ShouldFailOnStart = false

			// Simulate a failure during operation
			s6Service.Start(ctx)
			Expect(s6Service.GetStatus()).To(Equal(models.S6ServiceUp))
			s6Service.SimulateFailure(1)
			Expect(s6Service.GetStatus()).To(Equal(models.S6ServiceFailed))

			// Fail count should be incremented
			metrics := s6Service.GetMetrics()
			Expect(metrics["fail_count"]).To(Equal(1))
		})

		It("should update service configuration", func() {
			// Write initial config
			configPath, err := serviceManager.WriteConfig("test", "initial: config")
			Expect(err).NotTo(HaveOccurred())
			Expect(configPath).NotTo(BeEmpty())

			// Start service
			err = serviceManager.Start(ctx, "test")
			Expect(err).NotTo(HaveOccurred())
			Expect(serviceManager.IsRunning("test")).To(BeTrue())

			// Update config
			err = serviceManager.Update(ctx, "test", "updated: config")
			Expect(err).NotTo(HaveOccurred())

			// Service should be running again
			Expect(serviceManager.IsRunning("test")).To(BeTrue())
		})
	})
})
