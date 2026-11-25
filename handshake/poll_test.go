package handshake

import (
	"strings"
	"testing"
	"time"
)

// waitScript is a handshake whose whole job is one wait on Loaded, which the
// fake machine holds false until it is told to flip.
const waitScript = `
	opcua.wait_until({nodeId: "Loaded"}, v => v === true, %d%s);
	return opcua.success({waited: true});
`

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
		if n := b.count("read Loaded"); n < 4 {
			t.Fatalf("waited %d reads at a 25ms interval in 150ms", n)
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
		if n := b.count("read Loaded"); n < 2 || n > 6 {
			t.Fatalf("pollMs override ignored: %d reads in 300ms", n)
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
