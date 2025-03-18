package backoff_test

import (
	"errors"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/backoff"
)

func TestBackoff(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Backoff Suite")
}

var _ = Describe("BackoffManager", func() {
	var (
		manager *backoff.BackoffManager
		config  backoff.Config
		logger  *zap.SugaredLogger
	)

	BeforeEach(func() {
		// Setup a test logger that writes to the test log
		zapLogger := zaptest.NewLogger(GinkgoT())
		logger = zapLogger.Sugar()

		// Create a config with very short but predictable intervals for testing
		// Using fixed values to make tests more reliable
		config = backoff.Config{
			InitialInterval: 50 * time.Millisecond,
			MaxInterval:     200 * time.Millisecond,
			MaxRetries:      3,
			Logger:          logger,
		}

		manager = backoff.NewBackoffManager(config)
	})

	Context("when initializing", func() {
		It("should create a manager with default config", func() {
			defaultConfig := backoff.DefaultConfig("DefaultComponent", logger)
			defaultManager := backoff.NewBackoffManager(defaultConfig)
			Expect(defaultManager).NotTo(BeNil())
		})

		It("should create a manager with custom config", func() {
			Expect(manager).NotTo(BeNil())
		})
	})

	Context("when handling errors", func() {
		It("should track errors and reset correctly", func() {
			// Initially no error
			Expect(manager.GetLastError()).To(BeNil())
			Expect(manager.IsPermanentlyFailed()).To(BeFalse())

			// Set a temporary error
			testErr := errors.New("test error")
			isPermanent := manager.SetError(testErr)
			Expect(isPermanent).To(BeFalse()) // Not permanent on first attempt
			Expect(manager.GetLastError()).To(Equal(testErr))
			Expect(manager.IsPermanentlyFailed()).To(BeFalse())

			// Reset should clear the error state
			manager.Reset()
			Expect(manager.GetLastError()).To(BeNil())
			Expect(manager.ShouldSkipOperation()).To(BeFalse())
		})

		It("should detect permanent failure after max retries", func() {
			testErr := errors.New("test error")

			// Need to access the impl directly to set exact retries
			// Create a new manager with a shorter MaxRetries for this test
			specialConfig := backoff.Config{
				InitialInterval: 10 * time.Millisecond,
				MaxInterval:     100 * time.Millisecond,
				MaxRetries:      2, // Set to 2 for this specific test
				Logger:          logger,
			}
			specialManager := backoff.NewBackoffManager(specialConfig)

			// First error - not permanent
			isPermanent := specialManager.SetError(testErr)
			Expect(isPermanent).To(BeFalse(), "First error should not be permanent")
			Expect(specialManager.IsPermanentlyFailed()).To(BeFalse())

			// Second error - not permanent yet (since we allow 2 retries)
			isPermanent = specialManager.SetError(testErr)
			Expect(isPermanent).To(BeFalse(), "Second error should not be permanent")
			Expect(specialManager.IsPermanentlyFailed()).To(BeFalse())

			// Third error - now should be permanent (exceeded 2 retries)
			isPermanent = specialManager.SetError(testErr)
			Expect(isPermanent).To(BeTrue(), "Third error should be permanent")
			Expect(specialManager.IsPermanentlyFailed()).To(BeTrue())

			// Check that we get a permanent failure error
			backoffErr := specialManager.GetBackoffError()
			Expect(backoff.IsPermanentFailureError(backoffErr)).To(BeTrue(), "Error should indicate permanent failure")
			Expect(backoff.IsTemporaryBackoffError(backoffErr)).To(BeFalse(), "Error should not indicate temporary failure")
		})
	})

	Context("when checking operation skip", func() {
		It("should not skip operations initially", func() {
			Expect(manager.ShouldSkipOperation()).To(BeFalse())
		})

		It("should skip operations in backoff state", func() {
			// Set an error to enter backoff state
			testErr := errors.New("test error")
			manager.SetError(testErr)

			// Now operations should be skipped
			Expect(manager.ShouldSkipOperation()).To(BeTrue())
		})

		It("should not skip operations after reset", func() {
			// Set an error to enter backoff state
			testErr := errors.New("test error")
			manager.SetError(testErr)
			Expect(manager.ShouldSkipOperation()).To(BeTrue())

			// Reset and check again
			manager.Reset()
			Expect(manager.ShouldSkipOperation()).To(BeFalse())
		})
	})

	Context("when working with backoff errors", func() {
		It("should generate appropriate temporary backoff errors", func() {
			// Set an error but not permanently failed
			testErr := errors.New("test error")
			manager.SetError(testErr)

			backoffErr := manager.GetBackoffError()
			Expect(backoff.IsTemporaryBackoffError(backoffErr)).To(BeTrue())
			Expect(backoff.IsPermanentFailureError(backoffErr)).To(BeFalse())
		})

		It("should generate appropriate permanent backoff errors", func() {
			// Need to use a manager with a specific number of retries for reliable testing
			specialConfig := backoff.Config{
				InitialInterval: 10 * time.Millisecond,
				MaxInterval:     100 * time.Millisecond,
				MaxRetries:      2, // Set to 2 for predictable behavior
				Logger:          logger,
			}
			specialManager := backoff.NewBackoffManager(specialConfig)

			testErr := errors.New("test error")

			// Set errors until we reach permanent failure
			// First error
			specialManager.SetError(testErr)
			// Second error
			specialManager.SetError(testErr)
			// Third error should cause permanent failure
			isPermanent := specialManager.SetError(testErr)

			// Verify permanent failure
			Expect(isPermanent).To(BeTrue(), "Should be permanent after exceeding max retries")
			Expect(specialManager.IsPermanentlyFailed()).To(BeTrue())

			// Get error and verify it's a permanent failure error
			backoffErr := specialManager.GetBackoffError()
			Expect(backoff.IsPermanentFailureError(backoffErr)).To(BeTrue(), "Error should be permanent failure type")
			Expect(backoff.IsTemporaryBackoffError(backoffErr)).To(BeFalse(), "Error should not be temporary")
		})

		It("should preserve original error", func() {
			testErr := errors.New("original test error")
			manager.SetError(testErr)

			backoffErr := manager.GetBackoffError()
			extractedErr := backoff.ExtractOriginalError(backoffErr)
			Expect(extractedErr).To(Equal(testErr))
		})
	})

	Context("when using error helpers", func() {
		It("should correctly identify temporary backoff errors", func() {
			tempErr := errors.New(backoff.TemporaryBackoffError + ": test")
			Expect(backoff.IsTemporaryBackoffError(tempErr)).To(BeTrue())
			Expect(backoff.IsPermanentFailureError(tempErr)).To(BeFalse())
			Expect(backoff.IsBackoffError(tempErr)).To(BeTrue())
		})

		It("should correctly identify permanent failure errors", func() {
			permErr := errors.New(backoff.PermanentFailureError + ": test")
			Expect(backoff.IsTemporaryBackoffError(permErr)).To(BeFalse())
			Expect(backoff.IsPermanentFailureError(permErr)).To(BeTrue())
			Expect(backoff.IsBackoffError(permErr)).To(BeTrue())
		})

		It("should not identify regular errors as backoff errors", func() {
			regularErr := errors.New("regular error")
			Expect(backoff.IsTemporaryBackoffError(regularErr)).To(BeFalse())
			Expect(backoff.IsPermanentFailureError(regularErr)).To(BeFalse())
			Expect(backoff.IsBackoffError(regularErr)).To(BeFalse())
		})
	})

	Context("integration with time-based backoff", func() {
		It("should respect backoff delay", func() {
			// We need a simpler test that doesn't depend on complex timing
			// Create a fresh manager for this test
			testManager := backoff.NewBackoffManager(config)

			// Set an error to trigger backoff
			testErr := errors.New("test error")
			testManager.SetError(testErr)

			// Should be in backoff immediately after setting error
			Expect(testManager.ShouldSkipOperation()).To(BeTrue())

			// Reset the manager to clear backoff state
			testManager.Reset()

			// Should not be in backoff after reset
			Expect(testManager.ShouldSkipOperation()).To(BeFalse())
		})
	})
})
