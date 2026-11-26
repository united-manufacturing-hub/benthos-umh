// Copyright 2025 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

// LIB-4, the hole the JavaScript `catch` left: a cleanup whose write the
// machine refused has not handed the machine back, whether or not the block's
// script swallowed the refusal. The outcome is persistent — the flow must not
// retry a machine that was never returned to idle — and CleanupErr names the
// block and the refused operation.
func TestRun_IsPersistentWhenTheCleanupWriteWasRefusedAndTheBlockCaughtIt(t *testing.T) {
	b := &fakeBinding{values: map[string]any{},
		errs: map[string]error{"write:Abort": errors.New("machine refused the write")}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `return opcua.error_transient("not ready");`
		c.OnError = `try { opcua.write({nodeId: "Abort"}, true); } catch (err) { }`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v: a swallowed refusal still means the machine was not handed back",
			res.Outcome, res.Err)
	}
	if res.CleanupErr == nil {
		t.Fatalf("the swallowed refusal is not reported in CleanupErr")
	}
	if !strings.Contains(res.CleanupErr.Error(), "onError") {
		t.Fatalf("the cleanup failure does not name its block: %v", res.CleanupErr)
	}
	if !strings.Contains(res.CleanupErr.Error(), "Abort") {
		t.Fatalf("the cleanup failure does not name the refused operation: %v", res.CleanupErr)
	}
}

// The same hole on the call seam: a refused call_method in a cleanup block
// is a state-changing operation that did not reach the machine, caught or
// not, so the cleanup did not finish.
func TestRun_IsPersistentWhenTheCleanupCallWasRefusedAndTheBlockCaughtIt(t *testing.T) {
	caller := &fakeCaller{
		fakeBinding: fakeBinding{values: map[string]any{}},
		callErr:     errors.New("machine refused the call"),
	}
	engine := newTestEngine(t, caller, func(c *Config) {
		c.Script = `return opcua.error_transient("not ready");`
		c.OnError = `try { opcua.call_method({nodeId: "Reset"}, []); } catch (err) { }`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v: a swallowed call refusal still means the machine was not handed back",
			res.Outcome, res.Err)
	}
	if res.CleanupErr == nil || !strings.Contains(res.CleanupErr.Error(), "onError") {
		t.Fatalf("the cleanup failure must name onError, got %v", res.CleanupErr)
	}
}

// The other half of the rule, and why the fix counts only state-changing
// operations: a cleanup may probe. A read cannot change the machine, so a
// probe whose refusal the block caught has not failed to hand the machine
// back — this is how a script expresses an expected failure in a cleanup.
func TestRun_AProbeTheCleanupCaughtIsNotAFailedCleanup(t *testing.T) {
	b := &fakeBinding{values: map[string]any{},
		errs: map[string]error{"read:Flag": errors.New("machine refused the read")}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `return opcua.error_transient("not ready");`
		c.OnError = `
			try {
				if (opcua.read({nodeId: "Flag"}) === true) {
					opcua.write({nodeId: "Abort"}, false);
				}
			} catch (err) { } // the flag is not readable on every machine
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Transient {
		t.Fatalf("outcome %v, err %v: a caught read refusal does not mean the machine was not handed back",
			res.Outcome, res.Err)
	}
	if res.CleanupErr != nil {
		t.Fatalf("a probe the block caught is not a failed cleanup: %v", res.CleanupErr)
	}
}

// The write's counterpart to the probe rule above, and the door the
// round-3 review found unguarded: a cleanup write whose address does not
// parse never reached the machine, so the handback did not happen, caught
// or not. What records that is the dispatch boundary (cleanupEvidence);
// before this test nothing distinguished the parse's refusal from a probe
// the block carried on from, because no cleanup ever asked for a write
// with a typo in it.
func TestRun_ACleanupWriteWhoseAddressDoesNotParseIsAFailedCleanup(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `return opcua.error_transient("not ready");`
		c.OnError = `try { opcua.write({dataType: "Boolean"}, true); } catch (err) { }`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v: a cleanup write that never reached the machine did not hand it back",
			res.Outcome, res.Err)
	}
	if res.CleanupErr == nil || !strings.Contains(res.CleanupErr.Error(), "onError") {
		t.Fatalf("the cleanup failure must name onError, got %v", res.CleanupErr)
	}
	if !strings.Contains(res.CleanupErr.Error(), "parse") {
		t.Fatalf("the cleanup failure must name the refused parse, got: %v", res.CleanupErr)
	}
}

// LIB-11, the caught-failure hole on the success side: an onSuccess write the
// machine refused and the block swallowed still means the cleanup did not
// finish. The delivery stands, with CleanupErr naming onSuccess beside it.
func TestRun_ReportsASwallowedCleanupFailureAfterADelivery(t *testing.T) {
	b := &fakeBinding{values: map[string]any{},
		errs: map[string]error{"write:Ack": errors.New("machine refused the write")}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `return opcua.success({delivered: true});`
		c.OnSuccess = `try { opcua.write({nodeId: "Ack"}, false); } catch (err) { }`
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
	if !strings.Contains(res.CleanupErr.Error(), "Ack") {
		t.Fatalf("the cleanup failure does not name the refused operation: %v", res.CleanupErr)
	}
	if res.Err != nil {
		t.Fatalf("a delivery carries no fault: %v", res.Err)
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

// LIB-4, the wait_until half of the caught-failure hole: a wait that runs out
// inside a cleanup block means the block's job — returning the machine to a
// state it can be asked from — did not complete, caught or not, so the
// cleanup did not finish and the run is persistent. Before the fix the
// timeout borrowed the script's ending slot, the reset was refused as
// errEnded before it could be recorded, and the run
// reported the machine handed back when it never saw the reset.
func TestRun_IsPersistentWhenTheCleanupWaitTimedOutAndTheBlockCaughtIt(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Busy": true}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.PollInterval = 20 * time.Millisecond
		c.Script = `return opcua.error_transient("machine busy");`
		c.OnError = `
			try {
				opcua.wait_until({nodeId: "Busy"}, function(v){ return v === false; }, 200);
				opcua.write({nodeId: "Reset"}, true);
			} catch (err) { }
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v: a cleanup wait that timed out means the machine was not handed back",
			res.Outcome, res.Err)
	}
	if res.CleanupErr == nil {
		t.Fatalf("the swallowed wait timeout is not reported in CleanupErr")
	}
	for _, want := range []string{"onError", "Busy"} {
		if !strings.Contains(res.CleanupErr.Error(), want) {
			t.Fatalf("the cleanup failure must name %s, got: %v", want, res.CleanupErr)
		}
	}
	// The reset inside the same try never ran: the timeout throws, and a
	// throw skips the rest of the try, caught or not. The block that wants
	// to reset anyway catches the wait alone — that shape is the next test.
	if n := b.count("write Reset=true"); n != 0 {
		t.Fatalf("a throw inside the try skips the reset; it was attempted %d times", n)
	}
}

// The decision's other half: a wait timeout inside a cleanup ends nothing,
// so a block that catches the wait alone and resets still reaches the
// machine. "The machine is not touched after the script has ended" protects
// the transaction a script's first ending decided; a cleanup has no
// transaction left to protect — the outcome was decided before it ran — and
// the handback is its job, so the timeout records the failure without
// disarming the block's remaining writes.
func TestRun_TheCleanupCarriesOnAfterACaughtWaitTimeout(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Busy": true}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.PollInterval = 20 * time.Millisecond
		c.Script = `return opcua.error_transient("machine busy");`
		c.OnError = `
			try {
				opcua.wait_until({nodeId: "Busy"}, function(v){ return v === false; }, 200);
			} catch (err) { }
			opcua.write({nodeId: "Reset"}, true);
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v: the wait still timed out, so the machine was still not handed back",
			res.Outcome, res.Err)
	}
	if res.CleanupErr == nil {
		t.Fatalf("the caught wait timeout is not reported in CleanupErr")
	}
	if n := b.count("write Reset=true"); n != 1 {
		t.Fatalf("the reset after a caught timeout was attempted %d times, want once", n)
	}
}

// The same cleanup, written both ways, must agree — for every way a
// cleanup block's operations can fail. The block that catches the failure
// and carries on, and the block that lets it throw, describe the same
// machine state, so they must report the same outcome and the same
// CleanupErr. The first three doors were each closed with a guard at the
// failure site where they were found; door 6 — a wait_until called
// without its predicate — is the one that made the pattern undeniable: it
// returns before the machine is ever touched, so no guard inside the
// operation can see it, and the caught shape reported the machine handed
// back. Doors 7 and 8 are the two the return-value boundary could not see
// at all: a callback that throws unwinds the entry's Go frame by panic
// (7), and an argument goja cannot convert raises its TypeError at the
// entry, before the Go body runs (8). This agreement table is what the
// structural rule is measured against: the evidence is recorded where the
// block's operations are dispatched, so a failure route that has not been
// found yet inherits the rule instead of growing its own guard.
func TestRun_TheCaughtAndUncaughtCleanupFailuresAgree(t *testing.T) {
	// door is one way a cleanup block's operation can fail. call is the
	// statement as the block writes it; machine configures the fake so that
	// statement fails; names are the fragments the caught shape's CleanupErr
	// must mention (the recorded failure names the operation), and
	// uncaughtNames the uncaught shape's — there the block aborted, so
	// CleanupErr carries goja's own text for the throw, which names the door
	// only when the underlying error does (the timeout's message says "on
	// Busy"; a refused read's says only what the binding said). withCaller
	// binds the engine to a fakeCaller for the doors on the call seam.
	doors := []struct {
		door          string
		call          string
		machine       func(*fakeBinding)
		withCaller    bool
		names         []string
		uncaughtNames []string
	}{
		{
			door: "timeout",
			call: `opcua.wait_until({nodeId: "Busy"}, function(v){ return v === false; }, 200)`,
			machine: func(b *fakeBinding) {
				b.values["Busy"] = true // never false, so the wait runs out
			},
			names:         []string{"Busy"},
			uncaughtNames: []string{"Busy"},
		},
		{
			door: "refused read inside the wait",
			call: `opcua.wait_until({nodeId: "Busy"}, function(v){ return v === false; }, 200)`,
			machine: func(b *fakeBinding) {
				b.values["Busy"] = false
				b.errs = map[string]error{"read:Busy": errors.New("machine unreachable")}
			},
			names: []string{"Busy"},
		},
		{
			door:    "wait address does not parse",
			call:    `opcua.wait_until({}, function(v){ return v === false; }, 200)`,
			machine: func(b *fakeBinding) {},
			names:   []string{"parse"},
		},
		{
			// Door 6: a wait_until called without its predicate is a wrong
			// script that returns before the machine is touched, so the
			// operation log stays empty — and the caught shape reported the
			// machine handed back anyway, because the refusal never reached
			// any failure site a guard could sit at.
			door: "wait without its predicate",
			call: `opcua.wait_until({nodeId: "Idle"})`,
			machine: func(b *fakeBinding) {
				b.values["Idle"] = false
			},
			names:         []string{"wait_until", "predicate"},
			uncaughtNames: []string{"predicate"},
		},
		{
			// Door 7: a callback the block passed into an operation — the
			// wait's predicate — throws. goja unwinds the entry's Go frame by
			// panic, so the wrapper that would record the evidence never
			// runs. Measured on this branch: the caught shape reported the
			// machine handed back while the reset the throw skipped never
			// reached it.
			door: "a predicate that throws",
			call: `opcua.wait_until({nodeId: "Busy"}, function(v){ throw new Error("boom"); }, 1000)`,
			machine: func(b *fakeBinding) {
				b.values["Busy"] = false // the predicate throws on the first poll
			},
			names:         []string{"wait_until", "boom"},
			uncaughtNames: []string{"boom"},
		},
		{
			// Door 8: an argument goja cannot convert. The TypeError is
			// raised at the entry, before the Go body runs, so no wrapper
			// sees the failure. Every entry with a typed parameter has this
			// door; write's is the one measured. The ops log stays empty —
			// the machine was never asked for anything, and not handed back.
			door:          "an argument that does not convert",
			call:          `opcua.write(42, false)`,
			machine:       func(*fakeBinding) {},
			names:         []string{"write", "could not convert"},
			uncaughtNames: []string{"could not convert"},
		},
		{
			door:          "call_method address does not parse",
			call:          `opcua.call_method({}, [])`,
			machine:       func(*fakeBinding) {},
			withCaller:    true,
			names:         []string{"parse"},
			uncaughtNames: []string{"nodeId"},
		},
	}
	for _, d := range doors {
		d := d
		t.Run(d.door, func(t *testing.T) {
			run := func(catch bool) Result {
				var bind Binding[string]
				if d.withCaller {
					// call_method exists only where the binding can call, so
					// the call doors run against a fakeCaller. The machine
					// is the caller's own embedded fakeBinding — a copy
					// would move the lock (go vet) and the ops log with it.
					c := &fakeCaller{fakeBinding: fakeBinding{values: map[string]any{}}}
					bind = c
					d.machine(&c.fakeBinding)
				} else {
					b := &fakeBinding{values: map[string]any{}}
					bind = b
					d.machine(b)
				}
				onError := "\n\t\t\t\t" + d.call + ";\n\t\t\t\t\topcua.write({nodeId: \"Reset\"}, true);\n\t\t\t"
				if catch {
					onError = "try {" + onError + "} catch (err) { }"
				}
				engine := newTestEngine(t, bind, func(c *Config) {
					c.PollInterval = 20 * time.Millisecond
					c.Script = `return opcua.error_transient("machine busy");`
					c.OnError = onError
				})
				return engine.Run(t.Context(), map[string]any{})
			}

			caught, uncaught := run(true), run(false)
			if caught.Outcome != uncaught.Outcome {
				t.Fatalf("the caught %s is outcome %v, the uncaught one %v: the same cleanup must agree",
					d.door, caught.Outcome, uncaught.Outcome)
			}
			if (caught.CleanupErr == nil) != (uncaught.CleanupErr == nil) {
				t.Fatalf("the caught %s reports CleanupErr %v, the uncaught one %v: the same cleanup must agree",
					d.door, caught.CleanupErr, uncaught.CleanupErr)
			}
			for name, res := range map[string]Result{"caught": caught, "uncaught": uncaught} {
				t.Logf("%s: outcome=%v cleanupErr=%v", name, res.Outcome, res.CleanupErr)
				if res.Outcome != Persistent {
					t.Errorf("%s: outcome %v, err %v: a cleanup whose operation failed did not hand the machine back",
						name, res.Outcome, res.Err)
				}
				if res.CleanupErr == nil {
					t.Errorf("%s: the %s must be reported in CleanupErr", name, d.door)
					continue
				}
				want := d.names
				if name == "uncaught" {
					want = d.uncaughtNames
				}
				for _, fragment := range append([]string{"onError"}, want...) {
					if !strings.Contains(res.CleanupErr.Error(), fragment) {
						t.Errorf("%s: the cleanup failure must name %s, got: %v", name, fragment, res.CleanupErr)
					}
				}
			}
		})
	}
}

// The boundary the two tests above stand on: a cleanup wait that holds in
// time is not a failure. The block waited, the machine answered, the reset
// landed, so the handback finished and the fault stays retryable.
func TestRun_ACleanupWaitThatHeldIsNotAFailedCleanup(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Busy": false}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.PollInterval = 20 * time.Millisecond
		c.MaxOnErrorRuntime = 10 * time.Second // the wait below needs its budget
		c.Script = `return opcua.error_transient("machine busy");`
		c.OnError = `
			opcua.wait_until({nodeId: "Busy"}, function(v){ return v === false; }, 5000);
			opcua.write({nodeId: "Reset"}, true);
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Transient {
		t.Fatalf("outcome %v, err %v: a cleanup wait that held hands the machine back", res.Outcome, res.Err)
	}
	if res.CleanupErr != nil {
		t.Fatalf("a cleanup wait that held is not a failed cleanup: %v", res.CleanupErr)
	}
	if n := b.count("write Reset=true"); n != 1 {
		t.Fatalf("the reset was attempted %d times, want once", n)
	}
}
