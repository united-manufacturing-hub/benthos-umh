package handshake

import (
	"errors"
	"fmt"
	"testing"
)

// LIB-5: a fault marked ErrPersistent is persistent; an unmarked error is
// transient. The outcome follows the fault, not whether the script returned
// it or a call threw it.
func TestRun_TreatsAnUnmarkedBindingErrorAsTransient(t *testing.T) {
	t.Run("unmarked error is transient", func(t *testing.T) {
		b := &fakeBinding{values: map[string]any{},
			errs: map[string]error{"write:Apply": errors.New("machine unreachable")}}
		engine := newTestEngine(t, b, func(c *Config) {
			c.Script = `
				opcua.write({nodeId: "Apply"}, true);
				return opcua.success({done: true});
			`
		})
		res := engine.Run(t.Context(), map[string]any{})
		if res.Outcome != Transient {
			t.Fatalf("unmarked fault: outcome %v, err %v", res.Outcome, res.Err)
		}
		if res.Err == nil {
			t.Fatalf("a failed run carries its error")
		}
		if res.Data != nil {
			t.Fatalf("a failed run carries no data, got %v", res.Data)
		}
	})

	t.Run("persistent-marked error is persistent", func(t *testing.T) {
		b := &fakeBinding{values: map[string]any{},
			errs: map[string]error{"write:Apply": fmt.Errorf("BadTypeMismatch: %w", ErrPersistent)}}
		engine := newTestEngine(t, b, func(c *Config) {
			c.Script = `
				opcua.write({nodeId: "Apply"}, true);
				return opcua.success({done: true});
			`
		})
		res := engine.Run(t.Context(), map[string]any{})
		if res.Outcome != Persistent {
			t.Fatalf("marked fault: outcome %v, err %v", res.Outcome, res.Err)
		}
		if !errors.Is(res.Err, ErrPersistent) {
			t.Fatalf("Err does not carry the binding's own fault: %v", res.Err)
		}
	})
}

// LIB-8: the library never retries: one failed operation is attempted once.
// The output classifies and the flow retries.
func TestRun_AttemptsEachOperationOnce(t *testing.T) {
	t.Run("a failed write", func(t *testing.T) {
		b := &fakeBinding{values: map[string]any{},
			errs: map[string]error{"write:Apply": errors.New("machine unreachable")}}
		engine := newTestEngine(t, b, func(c *Config) {
			c.Script = `
				opcua.write({nodeId: "Apply"}, true);
				return opcua.success({done: true});
			`
		})
		res := engine.Run(t.Context(), map[string]any{})
		if res.Outcome != Transient {
			t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
		}
		if n := b.count("write Apply=true"); n != 1 {
			t.Fatalf("the library attempted a failed write %d times", n)
		}
	})

	t.Run("a failed read", func(t *testing.T) {
		b := &fakeBinding{values: map[string]any{},
			errs: map[string]error{"read:Ready": errors.New("machine unreachable")}}
		engine := newTestEngine(t, b, func(c *Config) {
			c.Script = `
				opcua.read({nodeId: "Ready"});
				return opcua.success({done: true});
			`
		})
		res := engine.Run(t.Context(), map[string]any{})
		if res.Outcome != Transient {
			t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
		}
		if n := b.count("read Ready"); n != 1 {
			t.Fatalf("the library attempted a failed read %d times", n)
		}
	})
}
