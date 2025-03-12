package utils

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("TransitionBackoffManager", func() {
	var manager *TransitionBackoffManager

	BeforeEach(func() {
		manager = NewTransitionBackoffManager()
	})

	Context("when calculating backoff duration", func() {
		It("should start with 1 second", func() {
			Expect(manager.CalculateBackoffDuration()).To(Equal(1 * time.Second))
		})

		It("should double the duration after each increment", func() {
			manager.IncrementRetryCount()
			Expect(manager.CalculateBackoffDuration()).To(Equal(2 * time.Second))

			manager.IncrementRetryCount()
			Expect(manager.CalculateBackoffDuration()).To(Equal(4 * time.Second))
		})

		It("should cap the duration at 1 minute", func() {
			// Set retry count high enough to exceed 1 minute
			for i := 0; i < 10; i++ {
				manager.IncrementRetryCount()
			}
			Expect(manager.CalculateBackoffDuration()).To(Equal(1 * time.Minute))
		})
	})

	Context("when checking if backoff has elapsed", func() {

		It("should return false immediately after increment", func() {
			manager.IncrementRetryCount()
			Expect(manager.ReconcileBackoffElapsed()).To(BeFalse())
		})

		It("should return true after waiting for backoff duration", func() {
			manager.lastTransitionAttempt = time.Now().Add(-2 * time.Second)
			Expect(manager.ReconcileBackoffElapsed()).To(BeTrue())
		})
	})

	Context("when resetting", func() {
		It("should reset retry count and update last transition time", func() {
			manager.IncrementRetryCount()
			manager.IncrementRetryCount()
			oldTime := manager.lastTransitionAttempt

			manager.Reset()

			Expect(manager.retryCount).To(Equal(1))
			Expect(manager.lastTransitionAttempt).To(BeTemporally(">", oldTime))
			Expect(manager.CalculateBackoffDuration()).To(Equal(1 * time.Second))
		})
	})
})
