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
	"strings"
	"testing"
	"time"
)

// waitScript is a handshake whose whole job is one wait on Loaded, which the
// fake machine holds false until it is told to flip.

// LIB-12: wait_until re-reads at the configured interval, which a script
// overrides per call with { pollMs }, and stops at its own timeout rather than
// the script's deadline.
func TestWaitUntil_StopsAtItsOwnTimeoutNotTheScriptDeadline(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Loaded": false}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.MaxScriptRuntime = 10 * time.Second // far past the wait's own timeout
		c.Script = `
			opcua.wait_until({nodeId: "Loaded"}, v => v === true, 100);
			return opcua.success({waited: true});
		`
	})

	start := time.Now()
	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Transient {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	if !strings.Contains(res.Err.Error(), "wait_until timed out") {
		t.Fatalf("Err does not name the wait: %v", res.Err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("the wait stopped at the script's deadline, not its own: %v", elapsed)
	}
}

// LIB-12 again: the interval is the configured one, or the call's { pollMs }.
func TestWaitUntil_PollsAtTheConfiguredInterval(t *testing.T) {
	t.Run("at the configured interval", func(t *testing.T) {
		b := &fakeBinding{values: map[string]any{"Loaded": false}}
		engine := newTestEngine(t, b, func(c *Config) {
			c.PollInterval = 25 * time.Millisecond
			c.Script = `
				opcua.wait_until({nodeId: "Loaded"}, v => v === true, 150);
				return opcua.success({waited: true});
			`
		})
		engine.Run(t.Context(), map[string]any{})
		// Seven reads at a 25ms interval in 150ms (t=0,25,...,150). The old
		// bound, n >= 4, let the mutation pass poll at half the rate.
		if n := b.count("read Loaded"); n < 6 || n > 9 {
			t.Fatalf("%d reads at a 25ms interval in 150ms: not the configured rate", n)
		}
	})

	t.Run("overridden per call", func(t *testing.T) {
		b := &fakeBinding{values: map[string]any{"Loaded": false}}
		engine := newTestEngine(t, b, func(c *Config) {
			c.PollInterval = 25 * time.Millisecond
			c.Script = `
				opcua.wait_until({nodeId: "Loaded"}, v => v === true, 300, { pollMs: 100 });
				return opcua.success({waited: true});
			`
		})
		engine.Run(t.Context(), map[string]any{})
		// Four reads at a 100ms poll in 300ms (t=0,100,200,300). The old
		// bound, 2..6, let the mutation pass poll at half the rate (3 reads).
		if n := b.count("read Loaded"); n < 4 || n > 5 {
			t.Fatalf("pollMs override ignored or wrong rate: %d reads in 300ms", n)
		}
	})

	t.Run("until the predicate holds", func(t *testing.T) {
		b := &fakeBinding{values: map[string]any{"Loaded": false}}
		b.flip("Loaded", 2) // true on the third read
		engine := newTestEngine(t, b, func(c *Config) {
			c.PollInterval = 10 * time.Millisecond
			c.Script = `
				opcua.wait_until({nodeId: "Loaded"}, v => v === true, 5000);
				return opcua.success({waited: true});
			`
		})
		res := engine.Run(t.Context(), map[string]any{})
		if res.Outcome != Delivered {
			t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
		}
		if n := b.count("read Loaded"); n != 3 {
			t.Fatalf("the wait re-read %d times, want 3", n)
		}
	})
}

// LIB-18: a wait_until that runs out of time is transient, and a script
// overrides that per call with { onTimeout: "persistent" }.
func TestWaitUntil_TimeoutIsTransient(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Loaded": false}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `
			opcua.wait_until({nodeId: "Loaded"}, v => v === true, 50);
			return opcua.success({waited: true});
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Transient {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	if res.Data != nil {
		t.Fatalf("a failed run carries no data, got %v", res.Data)
	}
}

// A wait_until without its predicate — left off, or a name that is not a
// function — must be a script fault a caller can read, not a panic out of
// Run. goja fills the missing func argument with a nil func and poll.go
// called it; in Benthos that panic is the process, and neither cleanup
// block runs, so the machine is left armed. Found by the round-2 review.
func TestWaitUntil_RefusesAMissingPredicate(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Loaded": false}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.OnError = `opcua.write({nodeId: "Apply"}, false);`
		c.Script = `
			opcua.wait_until({nodeId: "Loaded"});
			return opcua.success({waited: true});
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, want Persistent: a wait without a predicate is a wrong script (err %v)",
			res.Outcome, res.Err)
	}
	if res.Err == nil || !strings.Contains(res.Err.Error(), "predicate") {
		t.Fatalf("Err must say the predicate is missing, got %v", res.Err)
	}
	// The machine was handed back, so the panic did not skip the cleanup.
	if n := b.count("write Apply=false"); n != 1 {
		t.Fatalf("onError did not run (ops: %v)", b.snapshot())
	}
}

// LIB-18 again: only the script's author knows the flag is never coming.
func TestWaitUntil_OnTimeoutPersistentOverridesIt(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Loaded": false}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `
			opcua.wait_until({nodeId: "Loaded"}, v => v === true, 50, { onTimeout: "persistent" });
			return opcua.success({waited: true});
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	if !strings.Contains(res.Err.Error(), "wait_until timed out") {
		t.Fatalf("Err does not name the wait: %v", res.Err)
	}
}

// Ruling of 2026-11-25's night: wait_until clamps its own timeout to the time
// remaining before the script deadline, and when the clamp bites, the wait
// ends exactly as a wait that reached its own timeout does. The point is that
// the deadline must never fire in the middle of a transaction, because LIB-6
// makes that persistent and leaves the machine blocked.
//
// The wait's timeout comes from the message, because LIB-9 refuses a literal
// wait that does not clear the deadline: the literal check is the early
// friendly error, and the clamp is the safety mechanism for computed waits.
func TestWaitUntil_ClampsItsTimeoutToTheScriptDeadline(t *testing.T) {
	clampScript := func(onTimeout string) string {
		return `
			opcua.write({nodeId: "Apply"}, true);
			opcua.wait_until({nodeId: "Loaded"}, v => v === true, msg.payload.waitMs` + onTimeout + `);
			return opcua.success({waited: true});
		`
	}
	msg := map[string]any{"payload": map[string]any{"waitMs": 10000}}

	t.Run("transient when the clamp bites", func(t *testing.T) {
		b := &fakeBinding{values: map[string]any{"Loaded": false}}
		engine := newTestEngine(t, b, func(c *Config) {
			c.MaxScriptRuntime = 300 * time.Millisecond
			c.Script = clampScript("")
		})

		start := time.Now()
		res := engine.Run(t.Context(), msg)
		if res.Outcome != Transient {
			t.Fatalf("the clamp did not end the wait as its own timeout: outcome %v, err %v",
				res.Outcome, res.Err)
		}
		if !strings.Contains(res.Err.Error(), "wait_until timed out") {
			t.Fatalf("Err does not name the wait: %v", res.Err)
		}
		// The wait lasted until the clamp, not until the deadline.
		if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
			t.Fatalf("the clamp cut the wait short: %v", elapsed)
		}
	})

	t.Run("persistent when the call said so", func(t *testing.T) {
		b := &fakeBinding{values: map[string]any{"Loaded": false}}
		engine := newTestEngine(t, b, func(c *Config) {
			c.MaxScriptRuntime = 300 * time.Millisecond
			c.Script = clampScript(`, { onTimeout: "persistent" }`)
		})

		res := engine.Run(t.Context(), msg)
		if res.Outcome != Persistent {
			t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
		}
		if !strings.Contains(res.Err.Error(), "wait_until timed out") {
			t.Fatalf("Err does not name the wait: %v", res.Err)
		}
	})
}

// UAT-13's rule on the wait seam: a wait that timed out is itself an ending,
// so a script that catches its throw must not be able to start another wait
// under a transaction that has already been decided. The refusal is
// waitUntil's own `rs.end != endNone` guard; deleting it lets the second
// wait read the machine again, and nothing else in the suite noticed
// (COVERAGE.md's known-open mutant, still open on this tree).
func TestWaitUntil_AWaitUnderAnEndedTransactionIsRefused(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"A": false, "B": false}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.PollInterval = 20 * time.Millisecond
		c.Script = `
			try { opcua.wait_until({nodeId: "A"}, v => v === true, 60); } catch (e) { }
			try { opcua.wait_until({nodeId: "B"}, v => v === true, 60); } catch (e) { }
			return opcua.success({second: true});
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	// The first wait's timeout is the ending; the later success cannot
	// overwrite it, so the run is the first wait's transient timeout.
	if res.Outcome != Transient {
		t.Fatalf("outcome %v, err %v: the first wait's timeout must be the ending", res.Outcome, res.Err)
	}
	if !strings.Contains(res.Err.Error(), "wait_until timed out") {
		t.Fatalf("Err does not name the wait: %v", res.Err)
	}
	if n := b.count("read A"); n < 2 {
		t.Fatalf("the first wait never polled (ops: %v)", b.snapshot())
	}
	if n := b.count("read B"); n != 0 {
		t.Fatalf("the machine was read under a transaction that had already ended (ops: %v)", b.snapshot())
	}

	// readOp's own guard refuses the read even without waitUntil's, so the
	// one observable waitUntil's guard alone owns is the fault it keeps from
	// being recorded: a post-end wait whose address does not parse must not
	// record a binding fault, because `fail` keeps the latest one and a
	// deadline-stopped run would then blame a parse refusal that happened
	// after the transaction had already been decided.
	b2 := &fakeBinding{values: map[string]any{"A": false}}
	engine2 := newTestEngine(t, b2, func(c *Config) {
		c.MaxScriptRuntime = 250 * time.Millisecond
		c.PollInterval = 20 * time.Millisecond
		c.Script = `
			try { opcua.wait_until({nodeId: "A"}, v => v === true, 60); } catch (e) { }
			try { opcua.wait_until({dataType: "Boolean"}, v => v === true, 60); } catch (e) { }
			for (;;) { }
		`
	})
	res2 := engine2.Run(t.Context(), map[string]any{})
	if res2.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v: the deadline stopped the script", res2.Outcome, res2.Err)
	}
	if strings.Contains(res2.Err.Error(), "parse address") {
		t.Fatalf("a fault recorded after the transaction ended reached the result: %v", res2.Err)
	}
	if !strings.Contains(res2.Err.Error(), "script stopped before it ended") {
		t.Fatalf("Err does not name the stop: %v", res2.Err)
	}
}

// PollInterval is how often wait_until re-reads "when a call does not say";
// a config that does not set it must get the documented default, not a
// zero interval (a busy loop against the machine) and not silence.
func TestWaitUntil_APollIntervalOfZeroMeansTheDocumentedDefault(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Loaded": false}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.PollInterval = 0
		c.Script = `
			opcua.wait_until({nodeId: "Loaded"}, v => v === true, 600);
			return opcua.success({waited: true});
		`
	})

	engine.Run(t.Context(), map[string]any{})
	// Seven reads at the 100ms default in 600ms (t=0,100,...,600). A poll of
	// 300ms gives 3, a poll of 0 gives hundreds; both fail these bounds.
	if n := b.count("read Loaded"); n < 5 || n > 9 {
		t.Fatalf("%d reads in 600ms: PollInterval 0 does not mean the documented 100ms default", n)
	}
}

// number() must read pollMs in whichever Go type carried it, because the
// options object crosses the boundary twice: a JS number exports as int64 or
// float64, and a value that rode in from the message can be a plain Go int —
// vm.Set wraps the caller's map in a live lazy object whose Export() returns
// the original map, so the caller's own Go values reach the engine unchanged.
func TestWaitUntil_ReadsPollMsInEveryNumericFormItCanArrive(t *testing.T) {
	t.Run("a fractional JS number exports as float64", func(t *testing.T) {
		b := &fakeBinding{values: map[string]any{"Loaded": false}}
		engine := newTestEngine(t, b, func(c *Config) {
			c.PollInterval = 25 * time.Millisecond // the fallback if pollMs is dropped
			c.Script = `
				opcua.wait_until({nodeId: "Loaded"}, v => v === true, 300, { pollMs: 12.5 });
				return opcua.success({waited: true});
			`
		})
		engine.Run(t.Context(), map[string]any{})
		// ~21 reads at a 12.5ms poll in 300ms; ~12 at the configured fallback.
		if n := b.count("read Loaded"); n < 18 {
			t.Fatalf("%d reads in 300ms: a fractional pollMs was dropped", n)
		}
	})

	t.Run("an int that rode in from the message", func(t *testing.T) {
		b := &fakeBinding{values: map[string]any{"Loaded": false}}
		engine := newTestEngine(t, b, func(c *Config) {
			c.PollInterval = 0 // the fallback would be the 100ms default
			c.Script = `
				opcua.wait_until({nodeId: "Loaded"}, v => v === true, 420, msg.opts);
				return opcua.success({waited: true});
			`
		})
		engine.Run(t.Context(), map[string]any{"opts": map[string]any{"pollMs": 7}})
		// ~49 reads at a 7ms poll in 420ms (measured); ~5 at the default.
		if n := b.count("read Loaded"); n < 40 {
			t.Fatalf("%d reads in 420ms: a Go int pollMs from the message was dropped", n)
		}
	})
}

// clampWait's own edge: when less than the margin remains before the
// deadline, halving is what keeps the wait strictly inside it — a deadline
// that fires mid-transaction is persistent (LIB-6), and the clamp exists so
// it never does. Pinned directly, and end-to-end: a computed wait with less
// than the margin left still ends as its own timeout, transient (LIB-18),
// having polled.
func TestWaitUntil_AClampInsideTheMarginStillEndsBeforeTheDeadline(t *testing.T) {
	if got := clampWait(30 * time.Millisecond); got != 15*time.Millisecond {
		t.Fatalf("inside the margin the clamp halves the remainder, got %v", got)
	}
	if got := clampWait(60 * time.Millisecond); got != 10*time.Millisecond {
		t.Fatalf("outside the margin the clamp leaves the margin, got %v", got)
	}

	b := &fakeBinding{values: map[string]any{"Never": false}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.MaxScriptRuntime = 40 * time.Millisecond
		c.PollInterval = 10 * time.Millisecond
		c.Script = `
			opcua.write({nodeId: "Apply"}, true);
			opcua.wait_until({nodeId: "Never"}, v => v === true, msg.payload.waitMs);
			return opcua.success({waited: true});
		`
	})

	res := engine.Run(t.Context(), map[string]any{"payload": map[string]any{"waitMs": 10000}})
	if res.Outcome != Transient {
		t.Fatalf("outcome %v, err %v: a wait clamped inside the margin ends as its own timeout", res.Outcome, res.Err)
	}
	if !strings.Contains(res.Err.Error(), "wait_until timed out") {
		t.Fatalf("Err does not name the wait: %v", res.Err)
	}
	if n := b.count("read Never"); n < 2 {
		t.Fatalf("the clamped wait ended before it ever polled (ops: %v)", b.snapshot())
	}
}
