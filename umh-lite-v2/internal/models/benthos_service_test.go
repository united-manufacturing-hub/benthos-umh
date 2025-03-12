package models_test

import (
	"context"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/internal/models"
)

var _ = Describe("BenthosServiceManager", func() {
	var (
		manager    *models.BenthosServiceManager
		service1   *models.S6ServiceMock
		service2   *models.S6ServiceMock
		configDir  string
		ctx        context.Context
		cancelFunc context.CancelFunc
	)

	BeforeEach(func() {
		ctx, cancelFunc = context.WithTimeout(context.Background(), 5*time.Second)
		configDir = GinkgoT().TempDir()

		var err error
		manager, err = models.NewBenthosServiceManager(configDir)
		Expect(err).NotTo(HaveOccurred())

		service1 = models.NewS6ServiceMock("service1")
		service2 = models.NewS6ServiceMock("service2")

		manager.RegisterService("service1", service1)
		manager.RegisterService("service2", service2)
	})

	AfterEach(func() {
		cancelFunc()
	})

	Describe("Service registration and retrieval", func() {
		It("should register and retrieve services", func() {
			// Get registered service
			retrievedService, exists := manager.GetService("service1")
			Expect(exists).To(BeTrue())
			Expect(retrievedService).To(Equal(service1))

			// Try to get non-existent service
			_, exists = manager.GetService("non-existent")
			Expect(exists).To(BeFalse())
		})
	})

	Describe("Configuration management", func() {
		It("should write configurations to disk", func() {
			configPath, err := manager.WriteConfig("test-config", "test: content")
			Expect(err).NotTo(HaveOccurred())

			// Verify path
			expectedPath := filepath.Join(configDir, "test-config.yaml")
			Expect(configPath).To(Equal(expectedPath))

			// Verify file exists
			Expect(configPath).To(BeARegularFile())
		})
	})

	Describe("Service lifecycle management", func() {
		It("should start and stop services", func() {
			// Start service1
			err := manager.Start(ctx, "service1")
			Expect(err).NotTo(HaveOccurred())
			Expect(service1.IsRunning()).To(BeTrue())

			// Verify running state
			Expect(manager.IsRunning("service1")).To(BeTrue())

			// Try to start already running service
			err = manager.Start(ctx, "service1")
			Expect(err).To(HaveOccurred())

			// Stop service
			err = manager.Stop(ctx, "service1")
			Expect(err).NotTo(HaveOccurred())
			Expect(service1.IsRunning()).To(BeFalse())

			// Verify stopped state
			Expect(manager.IsRunning("service1")).To(BeFalse())

			// Try to stop already stopped service
			err = manager.Stop(ctx, "service1")
			Expect(err).To(HaveOccurred())
		})

		It("should update service configurations", func() {
			// Start service1
			err := manager.Start(ctx, "service1")
			Expect(err).NotTo(HaveOccurred())

			// Update service configuration
			err = manager.Update(ctx, "service1", "updated: config")
			Expect(err).NotTo(HaveOccurred())

			// Service should still be running after update
			Expect(service1.IsRunning()).To(BeTrue())

			// Try to update non-existent service
			err = manager.Update(ctx, "non-existent", "config")
			Expect(err).To(HaveOccurred())
		})

		It("should handle multiple services", func() {
			// Start both services
			err := manager.Start(ctx, "service1")
			Expect(err).NotTo(HaveOccurred())
			err = manager.Start(ctx, "service2")
			Expect(err).NotTo(HaveOccurred())

			// Both should be running
			Expect(service1.IsRunning()).To(BeTrue())
			Expect(service2.IsRunning()).To(BeTrue())

			// Get all running services
			runningServices := manager.GetAllRunningServices()
			Expect(runningServices).To(ContainElements("service1", "service2"))
			Expect(len(runningServices)).To(Equal(2))

			// Stop one service
			err = manager.Stop(ctx, "service1")
			Expect(err).NotTo(HaveOccurred())

			// One should be running, one stopped
			Expect(service1.IsRunning()).To(BeFalse())
			Expect(service2.IsRunning()).To(BeTrue())

			// Get all running services again
			runningServices = manager.GetAllRunningServices()
			Expect(runningServices).To(ContainElement("service2"))
			Expect(runningServices).NotTo(ContainElement("service1"))
			Expect(len(runningServices)).To(Equal(1))
		})
	})

	Describe("Error handling", func() {
		It("should handle service failures", func() {
			// Make service fail on start
			service1.ShouldFailOnStart = true

			// Try to start service
			err := manager.Start(ctx, "service1")
			Expect(err).To(HaveOccurred())

			// Service should be in failed state
			Expect(service1.GetStatus()).To(Equal(models.S6ServiceFailed))

			// Reset
			service1.ShouldFailOnStart = false

			// Start service successfully
			service1.Status = models.S6ServiceDown // Reset status
			err = manager.Start(ctx, "service1")
			Expect(err).NotTo(HaveOccurred())

			// Make service fail on stop
			service1.ShouldFailOnStop = true

			// Try to stop service
			err = manager.Stop(ctx, "service1")
			Expect(err).To(HaveOccurred())
		})
	})
})
