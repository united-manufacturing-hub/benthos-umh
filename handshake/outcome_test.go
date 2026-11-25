package handshake

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
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

// LIB-3: the outcome is decided before the cleanup runs, and a cleanup cannot
// turn a failure into a delivery. Here the cleanup succeeds completely and
// still does not rescue a persistent ending.
func TestRun_DecidesTheOutcomeBeforeCleanupRuns(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `
			opcua.write({nodeId: "ScriptDone"}, true);
			return opcua.error_persistent("the machine refused");
		`
		c.OnError = `opcua.write({nodeId: "CleanupDone"}, true);`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("a succeeded cleanup rescued the outcome: %v", res.Outcome)
	}
	if res.Data != nil {
		t.Fatalf("a failed run carries no data, got %v", res.Data)
	}
	// The script ran to its end before the cleanup started: the machine saw
	// the script's write first, and the outcome was already fixed by then.
	ops := b.snapshot()
	if len(ops) != 2 || ops[0] != "write ScriptDone=true" || ops[1] != "write CleanupDone=true" {
		t.Fatalf("script and cleanup interleaved: %v", ops)
	}
}

// LIB-3: a cleanup cannot turn a failure into a delivery, however well it
// goes.
func TestRun_CleanupCannotTurnAFailureIntoADelivery(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Ready": true}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `return opcua.error_transient("busy");`
		c.OnError = `
			opcua.read({nodeId: "Ready"});
			opcua.write({nodeId: "Apply"}, false);
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Transient {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	if res.Data != nil {
		t.Fatalf("a failed run carries no data, got %v", res.Data)
	}
	if res.Err == nil {
		t.Fatalf("the script's fault is still reported")
	}
}

// LIB-4: a run whose cleanup did not finish is persistent, whatever the fault
// was: the machine's state is unknown.
func TestRun_IsPersistentWhenTheCleanupDidNotFinish(t *testing.T) {
	b := &fakeBinding{values: map[string]any{},
		errs: map[string]error{"write:Apply": errors.New("machine unreachable")}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `return opcua.error_transient("busy");`
		c.OnError = `opcua.write({nodeId: "Apply"}, false);`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	if res.CleanupErr == nil {
		t.Fatalf("the result does not report the cleanup that failed")
	}
	if !strings.Contains(res.CleanupErr.Error(), "onError") {
		t.Fatalf("the cleanup failure does not name its block: %v", res.CleanupErr)
	}
}

// LIB-4: a transient fault whose cleanup finished is transient, so the message
// is safe to run again unchanged.
func TestRun_IsTransientWhenTheCleanupFinished(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `return opcua.error_transient("busy");`
		c.OnError = `opcua.write({nodeId: "Apply"}, false);`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Transient {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	if res.CleanupErr != nil {
		t.Fatalf("a finished cleanup is not a fault: %v", res.CleanupErr)
	}
}

// LIB-6: a script that exceeds its deadline is stopped, and the run is
// persistent: the deadline is configuration, so repeating cannot fix it.
func TestRun_IsPersistentWhenTheDeadlineStopsTheScript(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.MaxScriptRuntime = 100 * time.Millisecond
		c.Script = `for (;;) {}`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	if !errors.Is(res.Err, context.DeadlineExceeded) {
		t.Fatalf("Err does not name the deadline: %v", res.Err)
	}
}

// LIB-11: a cleanup that fails after a delivery does not change the outcome;
// the result still carries the delivery, with the cleanup failure beside it.
func TestRun_ReportsCleanupFailureAfterADeliveryWithoutChangingTheOutcome(t *testing.T) {
	b := &fakeBinding{values: map[string]any{},
		errs: map[string]error{"write:SuccessMarker": errors.New("machine unreachable")}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `return opcua.success({delivered: true});`
		c.OnSuccess = `opcua.write({nodeId: "SuccessMarker"}, true);`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Delivered {
		t.Fatalf("a failed cleanup changed a delivery: outcome %v, err %v", res.Outcome, res.Err)
	}
	if !reflect.DeepEqual(res.Data, map[string]any{"delivered": true}) {
		t.Fatalf("the delivery's data was lost: %v", res.Data)
	}
	if res.CleanupErr == nil || !strings.Contains(res.CleanupErr.Error(), "onSuccess") {
		t.Fatalf("the cleanup failure does not name its block: %v", res.CleanupErr)
	}
	if res.Err != nil {
		t.Fatalf("a delivery carries no fault: %v", res.Err)
	}
}
