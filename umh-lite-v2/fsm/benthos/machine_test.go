package benthos_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/fsm/benthos"
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

	It("should transition through states correctly when starting and stopping", func() {
		By("verifying initial state is stopped")
		Expect(instance.GetState()).To(Equal(benthos.StateStopped))
		Expect(instance.GetDesiredState()).To(Equal(benthos.StateStopped))

		By("setting desired state to running")
		err := instance.SetDesiredState(benthos.StateRunning)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetDesiredState()).To(Equal(benthos.StateRunning))

		By("first reconciliation: transitioning from stopped to starting")
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthos.StateStarting))

		By("second reconciliation: transitioning from starting to running")
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthos.StateRunning))

		By("setting desired state back to stopped")
		err = instance.SetDesiredState(benthos.StateStopped)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetDesiredState()).To(Equal(benthos.StateStopped))

		By("first reconciliation: transitioning from running to stopping")
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthos.StateStopping))

		By("second reconciliation: transitioning from stopping to stopped")
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthos.StateStopped))
	})

	It("should handle rapid desired state changes", func() {
		By("changing desired state multiple times before reconciliation")
		err := instance.SetDesiredState(benthos.StateRunning)
		Expect(err).NotTo(HaveOccurred())
		err = instance.SetDesiredState(benthos.StateStopped)
		Expect(err).NotTo(HaveOccurred())
		err = instance.SetDesiredState(benthos.StateRunning)
		Expect(err).NotTo(HaveOccurred())

		By("reconciling should move towards the final desired state")
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthos.StateStarting))

		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthos.StateRunning))
	})

	It("should handle desired state change during transition", func() {
		By("starting the transition to running")
		err := instance.SetDesiredState(benthos.StateRunning)
		Expect(err).NotTo(HaveOccurred())
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthos.StateStarting))

		By("changing desired state to stopped while in starting state")
		err = instance.SetDesiredState(benthos.StateStopped)
		Expect(err).NotTo(HaveOccurred())

		By("reconciling should keep the instance in starting state")
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthos.StateStopping))

		By("finally transition to stopped")
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthos.StateStopped))
	})

	It("should be idempotent when desired state matches current state", func() {
		By("setting desired state to current state (stopped)")
		err := instance.SetDesiredState(benthos.StateStopped)
		Expect(err).NotTo(HaveOccurred())

		By("reconciling multiple times should not change state")
		for i := 0; i < 3; i++ {
			err := instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthos.StateStopped))
		}

		By("transitioning to running")
		err = instance.SetDesiredState(benthos.StateRunning)
		Expect(err).NotTo(HaveOccurred())
		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthos.StateStarting))

		err = instance.Reconcile(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(instance.GetState()).To(Equal(benthos.StateRunning))

		By("setting desired state to current state (running)")
		err = instance.SetDesiredState(benthos.StateRunning)
		Expect(err).NotTo(HaveOccurred())

		By("reconciling multiple times should not change state")
		for i := 0; i < 3; i++ {
			err = instance.Reconcile(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(instance.GetState()).To(Equal(benthos.StateRunning))
		}
	})
})
