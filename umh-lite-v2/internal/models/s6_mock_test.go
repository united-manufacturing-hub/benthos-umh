package models_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/models"
)

var _ = Describe("S6ServiceMock", func() {
	var (
		service    *models.S6ServiceMock
		ctx        context.Context
		cancelFunc context.CancelFunc
	)

	BeforeEach(func() {
		ctx, cancelFunc = context.WithTimeout(context.Background(), 5*time.Second)
		service = models.NewS6ServiceMock("test-service")
	})

	AfterEach(func() {
		cancelFunc()
	})

	Describe("Service lifecycle", func() {
		It("should start and stop services", func() {
			// Initial state should be down
			Expect(service.GetStatus()).To(Equal(models.S6ServiceDown))
			Expect(service.IsRunning()).To(BeFalse())

			// Start service
			err := service.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Service should be running
			Expect(service.GetStatus()).To(Equal(models.S6ServiceUp))
			Expect(service.IsRunning()).To(BeTrue())

			// Stop service
			err = service.Stop(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Service should be stopped
			Expect(service.GetStatus()).To(Equal(models.S6ServiceDown))
			Expect(service.IsRunning()).To(BeFalse())
		})

		It("should prevent duplicate starts and stops", func() {
			// Start service
			err := service.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Try to start again
			err = service.Start(ctx)
			Expect(err).To(HaveOccurred())

			// Stop service
			err = service.Stop(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Try to stop again
			err = service.Stop(ctx)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Failure handling", func() {
		It("should handle startup failures", func() {
			// Configure service to fail on start
			service.ShouldFailOnStart = true

			// Try to start
			err := service.Start(ctx)
			Expect(err).To(HaveOccurred())
			Expect(service.GetStatus()).To(Equal(models.S6ServiceFailed))

			// Fail count should be incremented
			metrics := service.GetMetrics()
			Expect(metrics["fail_count"]).To(Equal(1))
		})

		It("should handle stop failures", func() {
			// Start service
			err := service.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Configure service to fail on stop
			service.ShouldFailOnStop = true

			// Try to stop
			err = service.Stop(ctx)
			Expect(err).To(HaveOccurred())

			// Service state should still be running
			Expect(service.GetStatus()).To(Equal(models.S6ServiceUp))
		})

		It("should handle simulated failures", func() {
			// Start service
			err := service.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Simulate a failure
			service.SimulateFailure(123)

			// Service should be in failed state
			Expect(service.GetStatus()).To(Equal(models.S6ServiceFailed))

			// Exit code should be set
			metrics := service.GetMetrics()
			Expect(metrics["exit_code"]).To(Equal(123))
		})
	})

	Describe("Readiness", func() {
		It("should handle readiness state", func() {
			// Start service (should be ready by default)
			err := service.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			metrics := service.GetMetrics()
			Expect(metrics["is_ready"]).To(BeTrue())

			// Simulate a failure (should not be ready)
			service.SimulateFailure(1)

			metrics = service.GetMetrics()
			Expect(metrics["is_ready"]).To(BeFalse())

			// Reset and start again
			service.Status = models.S6ServiceDown
			err = service.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Force readiness state
			service.SimulateReady()
			metrics = service.GetMetrics()
			Expect(metrics["is_ready"]).To(BeTrue())
		})
	})

	Describe("Metrics", func() {
		It("should track metrics properly", func() {
			// Start service multiple times
			service.Start(ctx)
			service.Stop(ctx)
			service.Start(ctx)
			service.Stop(ctx)
			service.Start(ctx)

			// Check metrics
			metrics := service.GetMetrics()
			Expect(metrics["start_count"]).To(Equal(3))
			Expect(metrics["stop_count"]).To(Equal(2))
			Expect(metrics["status"]).To(Equal(string(models.S6ServiceUp)))

			// Check uptime
			Expect(metrics["uptime_ms"]).To(BeNumerically(">", 0))
		})
	})

	Describe("Context handling", func() {
		It("should respect context cancellation during start", func() {
			// Set a long start delay
			service.StartDelay = 2 * time.Second

			// Create a short context
			shortCtx, shortCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer shortCancel()

			// Try to start with short context
			err := service.Start(shortCtx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("context"))
		})

		It("should respect context cancellation during stop", func() {
			// Start service
			err := service.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Set a long stop delay
			service.StopDelay = 2 * time.Second

			// Create a short context
			shortCtx, shortCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer shortCancel()

			// Try to stop with short context
			err = service.Stop(shortCtx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("context"))
		})
	})
})
