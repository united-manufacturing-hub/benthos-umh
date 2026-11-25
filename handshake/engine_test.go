package handshake

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// validConfig returns a configuration that can run, for tests that adapt it.
func validConfig() Config {
	return Config{
		Script:              `return opcua.success({});`,
		OnSuccess:           ``,
		OnError:             ``,
		MaxScriptRuntime:    time.Minute,
		MaxOnSuccessRuntime: time.Second,
		MaxOnErrorRuntime:   time.Second,
		PollInterval:        100 * time.Millisecond,
	}
}

// newTestEngine builds an engine around a fake machine and a configuration.
func newTestEngine(t *testing.T, b Binding[string], mutate func(*Config)) *Engine[string] {
	t.Helper()
	cfg := validConfig()
	if mutate != nil {
		mutate(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("test config does not validate: %v", err)
	}
	engine, err := New(cfg, b)
	if err != nil {
		t.Fatalf("New refused a valid config: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

// LIB-1: the script runs exactly once per write request. A repeat is a new
// delivery decided by the flow, never by the library.
func TestRun_RunsTheScriptOncePerMessage(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Ready": true}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `
			if (opcua.read({nodeId: "Ready"}) !== true) { return opcua.error_not_ready(); }
			opcua.write({nodeId: "Apply"}, true);
			return opcua.success({done: true});
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Delivered {
		t.Fatalf("first run: outcome %v, err %v", res.Outcome, res.Err)
	}
	res = engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Delivered {
		t.Fatalf("second run: outcome %v, err %v", res.Outcome, res.Err)
	}

	// Two write requests, so the script ran twice and only twice: one read and
	// one write each, and nothing the engine added on top.
	if n := b.count("read Ready"); n != 2 {
		t.Fatalf("script ran %d times, not once per message", n)
	}
	if n := b.count("write Apply=true"); n != 2 {
		t.Fatalf("script wrote %d times, not once per message", n)
	}
	if ops := b.snapshot(); len(ops) != 4 {
		t.Fatalf("machine saw %d operations, want 4: %v", len(ops), ops)
	}
}

// LIB-10: a script that returns success is Delivered, and the object it
// returns is carried in Result.Data.
func TestRun_DeliversWhenScriptReturnsSuccess(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Ready": true}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `
			const ready = opcua.read({nodeId: "Ready"});
			return opcua.success({ready: ready, tag: "42", ok: true});
		`
	})

	res := engine.Run(t.Context(), map[string]any{"payload": map[string]any{"value": 42}})
	if res.Outcome != Delivered {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	want := map[string]any{"ready": true, "tag": "42", "ok": true}
	if !reflect.DeepEqual(res.Data, want) {
		t.Fatalf("Data is %v, want %v", res.Data, want)
	}
	if res.Err != nil || res.CleanupErr != nil {
		t.Fatalf("a delivered run carries no fault: err %v, cleanup %v", res.Err, res.CleanupErr)
	}
}

// The message reaches the script as the flow decoded it, and a JSON number
// is json.Number in Go, the type benthos' structured messages carry. In the
// script it must be a number: a customer's read-back guard is
// v === msg.setpoint, and against a string-kind object it is always false
// (UAT scenario 12).
func TestRun_TheScriptSeesJSONNumbersAsNumbers(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `
			if (typeof msg.setpoint !== "number") {
				return opcua.error_persistent("msg.setpoint is a " + typeof msg.setpoint);
			}
			if (msg.setpoint !== 1.5) {
				return opcua.error_persistent("msg.setpoint is " + msg.setpoint);
			}
			if (msg.list[0] !== 2 || msg.nested.deep !== 3.25) {
				return opcua.error_persistent("nested numbers did not convert");
			}
			return opcua.success({seen: msg.setpoint});
		`
	})
	res := engine.Run(t.Context(), map[string]any{
		"setpoint": json.Number("1.5"),
		"list":     []any{json.Number("2")},
		"nested":   map[string]any{"deep": json.Number("3.25")},
	})
	if res.Outcome != Delivered {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	d, ok := res.Data.(map[string]any)
	if !ok || d["seen"] != 1.5 {
		t.Fatalf("Data is %#v", res.Data)
	}
}

// LIB-14: Validate compiles all three blocks; a script that does not parse is
// refused at deploy time, not against a machine.
func TestConfig_ValidateRefusesAScriptThatDoesNotCompile(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"script":    func(c *Config) { c.Script = `return {` },
		"onSuccess": func(c *Config) { c.OnSuccess = `opcua.write(` },
		"onError":   func(c *Config) { c.OnError = `function(){` },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("Validate accepted a %s that does not compile", name)
			}
		})
	}

	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate refused a config that can run: %v", err)
	}
}

// A cleanup block with no deadline would run unbounded: runCleanup reads a
// budget of zero or less as 'no limit', and nothing checked the two cleanup
// budgets, so an omitted YAML field plus a machine that never goes idle
// hung Run and Close forever with nothing logged. Found by the round-2
// review (item 3): the worst of the eight, because a config alone reaches
// it. Either block may be empty; a non-empty one must be budgeted.
func TestConfig_ValidateRefusesAnUnbudgetedCleanup(t *testing.T) {
	refuses := func(t *testing.T, mutate func(*Config), want ...string) {
		t.Helper()
		cfg := validConfig()
		mutate(&cfg)
		err := cfg.Validate()
		if err == nil {
			t.Fatalf("Validate accepted a cleanup block with no deadline")
		}
		for _, name := range want {
			if !strings.Contains(err.Error(), name) {
				t.Fatalf("the refusal must name %s, got: %v", name, err)
			}
		}
	}

	t.Run("onError with no budget", func(t *testing.T) {
		refuses(t, func(c *Config) {
			c.OnError = `opcua.write({nodeId: "Apply"}, false);`
			c.MaxOnErrorRuntime = 0
		}, "onError", "MaxOnErrorRuntime")
	})
	t.Run("onSuccess with no budget", func(t *testing.T) {
		refuses(t, func(c *Config) {
			c.OnSuccess = `opcua.write({nodeId: "Apply"}, false);`
			c.MaxOnSuccessRuntime = 0
		}, "onSuccess", "MaxOnSuccessRuntime")
	})
	t.Run("an empty block needs no budget", func(t *testing.T) {
		cfg := validConfig()
		cfg.MaxOnSuccessRuntime = 0
		cfg.MaxOnErrorRuntime = 0
		if err := cfg.Validate(); err != nil {
			t.Fatalf("an empty cleanup block needs no deadline: %v", err)
		}
	})
}

// LIB-9: a configuration whose literal waits do not clear the script deadline
// is refused. Literal waits only; a computed wait is beyond it (SCR-4).
func TestConfig_ValidateRefusesWaitsThatExceedTheScriptDeadline(t *testing.T) {
	tests := []struct {
		name    string
		script  string
		limit   time.Duration
		wantErr bool
	}{
		{
			name:    "one wait over the limit",
			script:  `opcua.wait_until(A, v => v === true, 5000); return opcua.success({});`,
			limit:   3 * time.Second,
			wantErr: true,
		},
		{
			name: "waits that add up over the limit",
			script: `opcua.wait_until(A, v => v === true, 4000);
				 opcua.wait_until(B, v => v === true, 7000);
				 return opcua.success({});`,
			limit:   10 * time.Second,
			wantErr: true,
		},
		{
			name:    "waits inside the limit",
			script:  `opcua.wait_until(A, v => v === true, 5000); return opcua.success({});`,
			limit:   6 * time.Second,
			wantErr: false,
		},
		{
			// A computed wait is a person's to check (SCR-4), not Validate's.
			name:    "computed wait is beyond it",
			script:  `opcua.wait_until(A, v => v === true, msg.payload.wait); return opcua.success({});`,
			limit:   time.Second,
			wantErr: false,
		},
		{
			// An address object with commas in it must not fool the scanner.
			name:    "address object with a comma",
			script:  `opcua.wait_until({nodeId: "ns=2;s=A", dataType: "Boolean"}, v => v === true, 100); return opcua.success({});`,
			limit:   time.Second,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Script = tt.script
			cfg.MaxScriptRuntime = tt.limit
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("Validate accepted a script whose waits exceed its deadline")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate refused a script whose waits clear its deadline: %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), "MaxScriptRuntime") {
				t.Fatalf("refusal does not name the deadline it violated: %v", err)
			}
		})
	}
}

// LIB-9's scanner, against three holes the round-2 review measured (item
// 4): a wait inside a comment is live code to the scanner, a backtick
// template literal desynchronises the argument splitter (the dangerous
// direction: a 600s wait under a 2s deadline was accepted), and the
// cleanup blocks' waits were never summed against their own budgets.
// The scanner is the friendly early error, not the safety mechanism, so
// the guard is that each hole is closed once, not over-built.
func TestConfig_ValidateScansOnlyLiveWaitsInEveryBlock(t *testing.T) {
	accepts := func(t *testing.T, mutate func(*Config)) {
		t.Helper()
		cfg := validConfig()
		mutate(&cfg)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate refused a config that can run: %v", err)
		}
	}
	refuses := func(t *testing.T, mutate func(*Config), want ...string) {
		t.Helper()
		cfg := validConfig()
		mutate(&cfg)
		err := cfg.Validate()
		if err == nil {
			t.Fatalf("Validate accepted a config whose waits exceed their deadline")
		}
		for _, name := range want {
			if !strings.Contains(err.Error(), name) {
				t.Fatalf("the refusal must name %s, got: %v", name, err)
			}
		}
	}

	t.Run("a wait inside a line comment is not live code", func(t *testing.T) {
		accepts(t, func(c *Config) {
			c.MaxScriptRuntime = 2 * time.Second
			c.Script = `
				// opcua.wait_until({nodeId: "ns=2;s=Loaded"}, v => v === true, 600000);
				return opcua.success({});
			`
		})
	})
	t.Run("a wait inside a block comment is not live code", func(t *testing.T) {
		accepts(t, func(c *Config) {
			c.MaxScriptRuntime = 2 * time.Second
			c.Script = `
				/* opcua.wait_until({nodeId: "ns=2;s=Loaded"}, v => v === true, 600000); */
				return opcua.success({});
			`
		})
	})
	t.Run("a backtick template literal does not desynchronise the scanner", func(t *testing.T) {
		refuses(t, func(c *Config) {
			c.MaxScriptRuntime = 2 * time.Second
			c.Script = "opcua.wait_until({nodeId: `A) B`}, v => v === true, 600000); return opcua.success({});"
		}, "MaxScriptRuntime")
	})
	t.Run("a string with a // inside is still scanned as code", func(t *testing.T) {
		refuses(t, func(c *Config) {
			c.MaxScriptRuntime = 2 * time.Second
			c.Script = `
				const where = "see // docs";
				opcua.wait_until({nodeId: "ns=2;s=Loaded"}, v => v === true, 600000);
				return opcua.success({where: where});
			`
		}, "MaxScriptRuntime")
	})
	t.Run("onError's waits are summed against its own budget", func(t *testing.T) {
		refuses(t, func(c *Config) {
			c.OnError = `opcua.wait_until({nodeId: "ns=2;s=Idle"}, v => v === true, 600000);`
			c.MaxOnErrorRuntime = 500 * time.Millisecond
		}, "onError", "MaxOnErrorRuntime")
	})
	t.Run("onSuccess's waits are summed against its own budget", func(t *testing.T) {
		refuses(t, func(c *Config) {
			c.OnSuccess = `opcua.wait_until({nodeId: "ns=2;s=Idle"}, v => v === true, 600000);`
			c.MaxOnSuccessRuntime = 500 * time.Millisecond
		}, "onSuccess", "MaxOnSuccessRuntime")
	})
	t.Run("a cleanup wait inside its budget is accepted", func(t *testing.T) {
		accepts(t, func(c *Config) {
			c.OnError = `opcua.wait_until({nodeId: "ns=2;s=Idle"}, v => v === true, 500);`
			c.MaxOnErrorRuntime = 5 * time.Second
		})
	})
}

// LIB-2: exactly one of onSuccess/onError runs after every run, including one
// the deadline stopped and one a cancelled context ended.
func TestRun_RunsOnSuccessAfterSuccessAndOnErrorAfterEverythingElse(t *testing.T) {
	cleanup := `
		opcua.write({nodeId: "Apply"}, true);
	`
	onSuccess := `
		opcua.write({nodeId: "SuccessMarker"}, true);
	`
	onError := `
		opcua.write({nodeId: "ErrorMarker"}, true);
	`
	ends := map[string]struct {
		script string
		want   string // the marker the machine must have seen
		not    string // the one it must not
	}{
		"success":          {`return opcua.success({});`, "SuccessMarker", "ErrorMarker"},
		"not_ready":        {`return opcua.error_not_ready();`, "ErrorMarker", "SuccessMarker"},
		"error_transient":  {`return opcua.error_transient("detail");`, "ErrorMarker", "SuccessMarker"},
		"error_persistent": {`return opcua.error_persistent("detail");`, "ErrorMarker", "SuccessMarker"},
		"a thrown fault":   {`throw new Error("script bug");`, "ErrorMarker", "SuccessMarker"},
	}

	for name, end := range ends {
		t.Run(name, func(t *testing.T) {
			b := &fakeBinding{values: map[string]any{}}
			engine := newTestEngine(t, b, func(c *Config) {
				c.Script = end.script
				c.OnSuccess = onSuccess
				c.OnError = onError
				_ = cleanup
			})
			res := engine.Run(t.Context(), map[string]any{})
			if res.Outcome == Delivered && end.want != "SuccessMarker" {
				t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
			}
			if n := b.count("write " + end.want + "=true"); n != 1 {
				t.Fatalf("the %s block did not run (ops: %v)", end.want, b.snapshot())
			}
			if n := b.count("write " + end.not + "=true"); n != 0 {
				t.Fatalf("both cleanup blocks ran (ops: %v)", b.snapshot())
			}
		})
	}
}

// LIB-2: onError also runs when the deadline stops the script, which a
// JavaScript finally cannot.
func TestRun_RunsOnErrorWhenTheScriptIsStoppedByItsDeadline(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Ready": true}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.MaxScriptRuntime = 100 * time.Millisecond
		c.OnError = `opcua.write({nodeId: "Apply"}, false);`
		c.Script = `
			opcua.read({nodeId: "Ready"});
			for (;;) {}
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	if n := b.count("write Apply=false"); n != 1 {
		t.Fatalf("onError did not run after the deadline (ops: %v)", b.snapshot())
	}
}

// LIB-2: onError runs when a cancelled context ends the run.
func TestRun_RunsOnErrorWhenTheContextIsCancelled(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.OnError = `opcua.write({nodeId: "Apply"}, false);`
		c.Script = `for (;;) {}`
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	res := engine.Run(ctx, map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	if !errors.Is(res.Err, context.DeadlineExceeded) {
		t.Fatalf("Err does not name the cancellation: %v", res.Err)
	}
	if res.CleanupErr != nil {
		t.Fatalf("the cleanup ran on its own clock and finished: %v", res.CleanupErr)
	}
	if n := b.count("write Apply=false"); n != 1 {
		t.Fatalf("onError did not run after the cancellation (ops: %v)", b.snapshot())
	}
}

// LIB-6: the deadline is configuration, so repeating cannot fix it — and a
// binding that honours its context and returns ctx's own error when the
// deadline fires mid-operation must not turn that stop into an unmarked,
// retryable fault. Found by UAT-4 (a deadline that fires mid-transaction
// was classed Transient); the fault's kind outranks nothing here, the
// stop does.
func TestRun_IsPersistentWhenTheDeadlineStopsAnOperation(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Ready": true}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.MaxScriptRuntime = 100 * time.Millisecond
		c.Script = `
			opcua.write({nodeId: "Apply"}, true);
			opcua.read({nodeId: "Slow"});
			return opcua.success({});
		`
	})
	// The read blocks until the run's context is done, then answers with
	// the context's own error, exactly as a binding that honours ctx does.
	b.blockRead = func(ctx context.Context) (ReadResponse, error) {
		<-ctx.Done()
		return ReadResponse{}, ctx.Err()
	}

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, want Persistent: a deadline that stops the script mid-operation is not retryable (err %v)", res.Outcome, res.Err)
	}
	if !errors.Is(res.Err, context.DeadlineExceeded) {
		t.Fatalf("Err does not name the deadline: %v", res.Err)
	}
}

// The ending functions abort the script by throwing, and a throw is
// catchable — so a script that catches its own ending must not be able to
// carry the transaction on. Found by UAT-13: a caught success throw let a
// script write to the machine again, under a run that had already been
// decided. The first ending wins, and the machine is not touched after it.
func TestRun_RefusesOperationsAfterTheScriptEnded(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Ready": true}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `
			if (opcua.read({nodeId: "Ready"}) !== true) { return opcua.error_not_ready(); }
			opcua.write({nodeId: "Apply"}, true);
			try {
				return opcua.success({done: true});
			} catch (e) {
				// The ending threw; a buggy script carries on.
			}
			opcua.write({nodeId: "Extra"}, 7);
			opcua.read({nodeId: "Ready"});
			return opcua.error_transient("unreachable in practice");
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	// The first ending decides, and is the outcome.
	if res.Outcome != Delivered {
		t.Fatalf("outcome %v, want Delivered: the first ending wins (err %v)", res.Outcome, res.Err)
	}
	if n := b.count("write Extra=7"); n != 0 {
		t.Fatalf("the machine was written after the transaction ended (ops: %v)", b.snapshot())
	}
	if n := b.count("read Ready"); n != 1 {
		t.Fatalf("the guard read before the ending is the only read (ops: %v)", b.snapshot())
	}
	if n := b.count("write Apply=true"); n != 1 {
		t.Fatalf("the committing write happened once (ops: %v)", b.snapshot())
	}
}

// The ending functions abort the script by throwing, and a throw is
// catchable — so a script that catches its ending and carries on is
// stopped by the deadline, not by the ending it recorded on the way.
// That run is not a delivery: LIB-2 puts a deadline-stopped run under
// onError, LIB-6 makes it persistent, and LIB-3 decides the outcome when
// the script ends — a script the deadline killed never ended for the
// ending it recorded first. Found by the round-2 review's probe of
// UAT-13: try { success } catch {} for(;;){} came back Delivered and ran
// onSuccess on a machine whose transaction was cut in half.
func TestRun_TheDeadlineOutranksAnEndingTheScriptNeverStoppedFor(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.MaxScriptRuntime = 150 * time.Millisecond
		c.OnSuccess = `opcua.write({nodeId: "SuccessMarker"}, true);`
		c.OnError = `opcua.write({nodeId: "ErrorMarker"}, true);`
		c.Script = `
			try {
				return opcua.success({x: 1});
			} catch (e) {}
			for (;;) {}
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, want Persistent: the deadline stopped the script (err %v)", res.Outcome, res.Err)
	}
	if res.Data != nil {
		t.Fatalf("a run the deadline stopped carries no data, got %v", res.Data)
	}
	if !errors.Is(res.Err, context.DeadlineExceeded) {
		t.Fatalf("Err does not name the deadline: %v", res.Err)
	}
	if n := b.count("write SuccessMarker=true"); n != 0 {
		t.Fatalf("onSuccess ran for a run the deadline stopped (ops: %v)", b.snapshot())
	}
	if n := b.count("write ErrorMarker=true"); n != 1 {
		t.Fatalf("onError did not run (ops: %v)", b.snapshot())
	}
}

// No panic from a script or a binding leaves Run: the recovered value is a
// persistent fault, and both cleanups still run, because a binding is
// protocol code that can panic under a machine's bad answer as easily as a
// script can call something wrongly. Found by the round-2 review, which
// watched a nil predicate panic out of Run with the machine left armed;
// grep found no recover anywhere in the package.
func TestRun_ABindingPanicIsAPersistentFaultNotACrash(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.OnError = `opcua.write({nodeId: "Apply"}, false);`
		c.Script = `
			opcua.write({nodeId: "Setpoint"}, 42);
			opcua.read({nodeId: "Loaded"});
			return opcua.success({});
		`
	})
	b.blockRead = func(context.Context) (ReadResponse, error) {
		panic("machine answered a 0x0f status the driver does not map")
	}

	res := engine.Run(t.Context(), map[string]any{}) // must not take the process with it
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, want Persistent: a panic is not a delivery and not a retry (err %v)",
			res.Outcome, res.Err)
	}
	if res.Err == nil || !strings.Contains(res.Err.Error(), "panicked") {
		t.Fatalf("Err must say the run panicked, got %v", res.Err)
	}
	if n := b.count("write Setpoint=42"); n != 1 {
		t.Fatalf("the write before the panic happened once (ops: %v)", b.snapshot())
	}
	if n := b.count("write Apply=false"); n != 1 {
		t.Fatalf("onError did not run after the panic (ops: %v)", b.snapshot())
	}
}
