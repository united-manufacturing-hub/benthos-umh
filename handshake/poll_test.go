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

// wait_until re-reads at the configured interval, which a script
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

// The interval is the configured one, or the call's { pollMs }.
//
// A sleep never ends early, so each read count has a hard upper bound, which
// catches a wait that polls faster than it was told. The lower bounds are
// generous, because a loaded runner under -race oversleeps; each one still
// sits above what half the rate would produce.
func TestWaitUntil_PollsAtTheConfiguredInterval(t *testing.T) {
	t.Run("at the configured interval", func(t *testing.T) {
		b := &fakeBinding{values: map[string]any{"Loaded": false}}
		engine := newTestEngine(t, b, func(c *Config) {
			c.PollInterval = 25 * time.Millisecond
			c.Script = `
				opcua.wait_until({nodeId: "Loaded"}, v => v === true, 500);
				return opcua.success({waited: true});
			`
		})
		engine.Run(t.Context(), map[string]any{})
		// At most 20 reads at a 25ms interval in 500ms (t=0,25,...,475; a
		// read starting at t=500 has no time left). Half the rate gives at
		// most 10.
		if n := b.count("read Loaded"); n < 11 || n > 21 {
			t.Fatalf("%d reads at a 25ms interval in 500ms: not the configured rate", n)
		}
	})

	t.Run("overridden per call", func(t *testing.T) {
		b := &fakeBinding{values: map[string]any{"Loaded": false}}
		engine := newTestEngine(t, b, func(c *Config) {
			c.PollInterval = 25 * time.Millisecond
			c.Script = `
				opcua.wait_until({nodeId: "Loaded"}, v => v === true, 1000, { pollMs: 100 });
				return opcua.success({waited: true});
			`
		})
		engine.Run(t.Context(), map[string]any{})
		// At most 10 reads at a 100ms poll in 1000ms: t=0, 100, ..., 900.
		// There is no read at t=1000, because each read is bounded by the
		// wait's own end and a read starting then has no time to complete.
		// Half the rate gives at most 5; the configured 25ms gives about 40.
		if n := b.count("read Loaded"); n < 6 || n > 11 {
			t.Fatalf("pollMs override ignored or wrong rate: %d reads in 1000ms, want 6 to 11", n)
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

// A wait_until that runs out of time is transient, and a script
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

// A wait_until without its predicate must be a script fault a caller can
// read, not a panic out of Run: in Benthos that panic ends the process, and
// neither cleanup block runs, so the machine is left armed.
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

// onTimeout persistent overrides it: only the script's author knows the flag is never coming.
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

// wait_until clamps its own timeout to the time remaining before the script
// deadline, and when the clamp bites, the wait ends exactly as a wait that
// reached its own timeout does. The point is that the deadline must never
// fire in the middle of a transaction, because that is persistent and
// leaves the machine blocked.
//
// The wait's timeout comes from the message, because Validate refuses a
// literal wait that does not clear the deadline: the literal check is the
// early friendly error, and the clamp is the safety mechanism for computed
// waits.
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
		// The wait lasted until the clamp, 50ms before the 300ms deadline,
		// however long the run took to reach it: the clamp is measured from
		// the deadline, and a sleep never ends early. The Transient outcome
		// above shows it did not last until the deadline.
		if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
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

// A wait that timed out is itself an ending, so a script that catches its
// throw must not be able to start another wait under a transaction that has
// already been decided. address refuses every operation once the run has
// ended, before it parses the address.
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
	if n := b.count("read A"); n < 1 {
		t.Fatalf("the first wait never polled (ops: %v)", b.snapshot())
	}
	if n := b.count("read B"); n != 0 {
		t.Fatalf("the machine was read under a transaction that had already ended (ops: %v)", b.snapshot())
	}

	// Because address refuses before it parses, a post-end wait whose
	// address does not parse records no binding fault, so the
	// deadline-stopped run below does not blame that parse refusal, which
	// happened after the transaction had been decided.
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
			opcua.wait_until({nodeId: "Loaded"}, v => v === true, 1000);
			return opcua.success({waited: true});
		`
	})

	engine.Run(t.Context(), map[string]any{})
	// At most 10 reads at the 100ms default in 1000ms (t=0,100,...,900). A
	// poll of 300ms gives at most 4, a poll of 0 or 1ms gives hundreds; both
	// fail these bounds. The lower bound leaves room for a loaded runner.
	if n := b.count("read Loaded"); n < 5 || n > 11 {
		t.Fatalf("%d reads in 1000ms: PollInterval 0 does not mean the documented 100ms default", n)
	}
}

// number must read pollMs in whichever Go type carried it: a JS number
// exports as int64 or float64, and a value that rode in from the message can
// be a plain Go int, because jsValue copies the message's containers but
// leaves an int as it is.
//
// The fallback interval is far slower than pollMs in both cases, so the lower
// bounds can leave a loaded runner plenty of room and still tell a dropped
// pollMs apart.
func TestWaitUntil_ReadsPollMsInEveryNumericFormItCanArrive(t *testing.T) {
	t.Run("a fractional JS number exports as float64", func(t *testing.T) {
		b := &fakeBinding{values: map[string]any{"Loaded": false}}
		engine := newTestEngine(t, b, func(c *Config) {
			c.PollInterval = 100 * time.Millisecond // the fallback if pollMs is dropped
			c.Script = `
				opcua.wait_until({nodeId: "Loaded"}, v => v === true, 300, { pollMs: 12.5 });
				return opcua.success({waited: true});
			`
		})
		engine.Run(t.Context(), map[string]any{})
		// At most 24 reads at a 12.5ms poll in 300ms; at most 3 at the
		// fallback.
		if n := b.count("read Loaded"); n < 8 {
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
		// At most 60 reads at a 7ms poll in 420ms; at most 5 at the default.
		if n := b.count("read Loaded"); n < 15 {
			t.Fatalf("%d reads in 420ms: a Go int pollMs from the message was dropped", n)
		}
	})
}

// clampWait's own edge: when less than the margin remains before the
// deadline, halving is what keeps the wait strictly inside it, because a
// deadline that fires mid-transaction is persistent. Pinned directly, and
// end-to-end: a computed wait with less than the margin left still ends as
// its own timeout, transient, having polled.
//
// The end-to-end budget is just under the margin, so the halving branch runs
// however long the run takes to reach the wait, and the time before the wait
// can grow on a loaded runner without eating the whole budget.
func TestWaitUntil_AClampInsideTheMarginStillEndsBeforeTheDeadline(t *testing.T) {
	if got := clampWait(30 * time.Millisecond); got != 15*time.Millisecond {
		t.Fatalf("inside the margin the clamp halves the remainder, got %v", got)
	}
	if got := clampWait(60 * time.Millisecond); got != 10*time.Millisecond {
		t.Fatalf("outside the margin the clamp leaves the margin, got %v", got)
	}

	b := &fakeBinding{values: map[string]any{"Never": false}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.MaxScriptRuntime = 49 * time.Millisecond
		c.PollInterval = 5 * time.Millisecond
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
	if n := b.count("read Never"); n < 1 {
		t.Fatalf("the clamped wait ended before it ever polled (ops: %v)", b.snapshot())
	}
}
