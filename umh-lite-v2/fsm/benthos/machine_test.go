package benthos_test

import (
	"context"
	"errors"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/fsm/benthos"
	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/fsm/utils"
)

func TestBenthosFsm(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Benthos FSM Suite")
}

var _ = Describe("Benthos FSM State Transitions", func() {
	var (
		instance *benthos.BenthosInstance
		ctx      context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		instance = benthos.NewBenthosInstance("test-instance", nil)
		instance.RegisterCallbacks()
	})

	Context("Lifecycle States", func() {
		It("should handle initial creation correctly", func() {
			By("verifying initial state is LifecycleStateToBeCreated")
			Expect(instance.GetState()).To(Equal(utils.LifecycleStateToBeCreated))

			By("reconciling should transition from to_be_created to creating")
			err := instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(utils.LifecycleStateCreating))
		})

		It("should handle removal from Stopped state", func() {
			By("getting to stopping state")
			Expect(instance.GetState()).To(Equal(utils.LifecycleStateToBeCreated))
			err := instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(utils.LifecycleStateCreating))
			err = instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthos.OperationalStateStopped))

			By("initiating removal from Stopped state")
			err = instance.Remove(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(utils.LifecycleStateRemoving))

			By("reconciling should handle the removal")
			err = instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(utils.LifecycleStateRemoved))
		})

		It("should handle removal from Running state", func() {
			By("getting to stopping state")
			Expect(instance.GetState()).To(Equal(utils.LifecycleStateToBeCreated))
			err := instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(utils.LifecycleStateCreating))
			err = instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthos.OperationalStateStopped))

			By("setting desired state to running")
			err = instance.SetDesiredState(benthos.OperationalStateRunning)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetDesiredState()).To(Equal(benthos.OperationalStateRunning))

			By("first reconciliation: transitioning from stopped to starting")
			err = instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthos.OperationalStateStarting))

			By("second reconciliation: instance is now running")
			instance.SetObservedState(true) // Simulate instance running
			err = instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthos.OperationalStateRunning))

			By("initiating removal from Running state")
			err = instance.Remove(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(utils.LifecycleStateRemoving))

			By("reconciling should handle the removal")
			err = instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(utils.LifecycleStateRemoved))
		})

	})

	Context("Operational States", func() {
		It("should transition through states correctly when starting and stopping", func() {
			By("getting to stopping state")
			Expect(instance.GetState()).To(Equal(utils.LifecycleStateToBeCreated))
			err := instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(utils.LifecycleStateCreating))
			err = instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthos.OperationalStateStopped))

			By("setting desired state to running")
			err = instance.SetDesiredState(benthos.OperationalStateRunning)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetDesiredState()).To(Equal(benthos.OperationalStateRunning))

			By("first reconciliation: transitioning from stopped to starting")
			err = instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthos.OperationalStateStarting))

			By("second reconciliation: instance is now running")
			instance.SetObservedState(true) // Simulate instance running
			err = instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthos.OperationalStateRunning))

			By("setting desired state back to stopped")
			err = instance.SetDesiredState(benthos.OperationalStateStopped)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetDesiredState()).To(Equal(benthos.OperationalStateStopped))

			By("first reconciliation: transitioning from running to stopping")
			err = instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthos.OperationalStateStopping))

			By("second reconciliation: instance is now stopped")
			instance.SetObservedState(false) // Simulate instance stopped
			err = instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthos.OperationalStateStopped))
		})
	})

	Context("Error Handling and Backoff", func() {
		It("should handle errors and respect backoff", func() {
			Skip("TODO: provoke an actual error that also sets teh suspendedTime, e.g., through a mock service")
			Expect(instance.GetState()).To(Equal(utils.LifecycleStateToBeCreated))

			// Set up an error condition
			instance.SetError(errors.New("test error"))
			// TODO: provoke an actual error that also sets teh suspendedTime, e.g., through a mock service

			By("respecting backoff period")
			err := instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(utils.LifecycleStateToBeCreated)) // State shouldn't change

			By("allowing retry after backoff")
			time.Sleep(time.Millisecond * 200) // Simulate backoff elapsed
			instance.ClearError()              // Clear error as if backoff elapsed
			err = instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(utils.LifecycleStateCreating))
		})
	})
})
