package handshake

import (
	"context"
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
	if n := b.count("write Apply=false"); n != 1 {
		t.Fatalf("onError did not run after the cancellation (ops: %v)", b.snapshot())
	}
}
