package s6

import (
	"context"
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestS6Service(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "S6 Service Suite")
}

var _ = Describe("S6 Service", func() {
	var (
		mockService *MockService
		ctx         context.Context
		testPath    string
	)

	BeforeEach(func() {
		mockService = NewMockService()
		ctx = context.Background()
		testPath = "/tmp/test-s6-service"

		// Cleanup any test directories if they exist
		os.RemoveAll(testPath)
	})

	AfterEach(func() {
		// Cleanup
		os.RemoveAll(testPath)
	})

	It("should track method calls in the mock implementation", func() {
		Expect(mockService.CreateCalled).To(BeFalse())
		mockService.Create(ctx, testPath, S6ServiceConfig{})
		Expect(mockService.CreateCalled).To(BeTrue())

		Expect(mockService.StartCalled).To(BeFalse())
		mockService.Start(ctx, testPath)
		Expect(mockService.StartCalled).To(BeTrue())

		Expect(mockService.StopCalled).To(BeFalse())
		mockService.Stop(ctx, testPath)
		Expect(mockService.StopCalled).To(BeTrue())

		Expect(mockService.RestartCalled).To(BeFalse())
		mockService.Restart(ctx, testPath)
		Expect(mockService.RestartCalled).To(BeTrue())

		Expect(mockService.StatusCalled).To(BeFalse())
		mockService.Status(ctx, testPath)
		Expect(mockService.StatusCalled).To(BeTrue())

		Expect(mockService.ExistsCalled).To(BeFalse())
		mockService.ServiceExists(ctx, testPath)
		Expect(mockService.ExistsCalled).To(BeTrue())
	})

	It("should manage service state in the mock implementation", func() {
		// Service should not exist initially
		exists, _ := mockService.ServiceExists(ctx, testPath)
		Expect(exists).To(BeFalse())

		// Create service should make it exist
		mockService.Create(ctx, testPath, S6ServiceConfig{})
		exists, _ = mockService.ServiceExists(ctx, testPath)
		Expect(exists).To(BeTrue())

		// Set the service state to down initially
		mockService.ServiceStates[testPath] = ServiceInfo{
			Status: ServiceDown,
		}

		// Get status should return the set state
		info, err := mockService.Status(ctx, testPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Status).To(Equal(ServiceDown))

		// Start service should change state to up
		mockService.Start(ctx, testPath)
		info, _ = mockService.Status(ctx, testPath)
		Expect(info.Status).To(Equal(ServiceUp))

		// Stop service should change state to down
		mockService.Stop(ctx, testPath)
		info, _ = mockService.Status(ctx, testPath)
		Expect(info.Status).To(Equal(ServiceDown))

		// Restart service should change state to up (after briefly being restarting)
		mockService.Restart(ctx, testPath)
		info, _ = mockService.Status(ctx, testPath)
		Expect(info.Status).To(Equal(ServiceUp))

		// Remove service should make it not exist
		mockService.Remove(ctx, testPath)
		exists, _ = mockService.ServiceExists(ctx, testPath)
		Expect(exists).To(BeFalse())

		// Status should not be available after removal
		_, err = mockService.Status(ctx, testPath)
		Expect(err).NotTo(HaveOccurred()) // No error, but...
		info = mockService.StatusResult   // Should return the default result
		Expect(info.Status).To(Equal(ServiceUnknown))
	})
})
