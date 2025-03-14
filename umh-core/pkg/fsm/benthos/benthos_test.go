package benthos

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
)

func TestBenthos(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Benthos Suite")
}

var _ = Describe("Benthos FSM", func() {
	// TODO: Implement proper tests for the new Benthos FSM implementation
	// These will include:
	// - Testing state transitions
	// - Testing reconciliation logic
	// - Testing health monitoring
	// - Testing configuration validation

	It("should validate lifecycle states", func() {
		Expect(internal_fsm.LifecycleStateToBeCreated).To(Equal("to_be_created"))
		Expect(internal_fsm.LifecycleStateCreating).To(Equal("creating"))
		Expect(internal_fsm.LifecycleStateRemoving).To(Equal("removing"))
		Expect(internal_fsm.LifecycleStateRemoved).To(Equal("removed"))
	})

	It("should validate operational states", func() {
		Expect(OperationalStateStarting).To(Equal("starting"))
		Expect(OperationalStateRunning).To(Equal("running"))
		Expect(OperationalStateStopping).To(Equal("stopping"))
		Expect(OperationalStateStopped).To(Equal("stopped"))
	})
})
