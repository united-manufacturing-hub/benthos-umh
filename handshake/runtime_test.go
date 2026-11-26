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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// LIB-7: the script's own variables are reset for every run. A script's
// globals must not reach the next message, which is the fact the spec's
// no-memory limit rests on.
func TestRun_CarriesNothingBetweenRuns(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `
			if (typeof globalThis.x === "undefined") {
				globalThis.x = 1;
				return opcua.error_transient("first run");
			}
			return opcua.success({carried: globalThis.x});
		`
	})

	// The first run leaves a global behind; the second run must not see it.
	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Transient {
		t.Fatalf("first run: outcome %v, err %v", res.Outcome, res.Err)
	}
	res = engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Transient {
		t.Fatalf("a global reached the next message: outcome %v, data %v", res.Outcome, res.Data)
	}
}

// LIB-13: the value a binding returns reaches the script unchanged. The
// binding decides what counts as a value and what counts as an error.
func TestRead_PassesTheBindingsValueThroughUnchanged(t *testing.T) {
	values := map[string]any{
		"flag":  true,
		"count": int64(-5),
		"name":  "keep me",
		"ratio": 1.5,
	}
	for addr, want := range values {
		t.Run(fmt.Sprintf("%T", want), func(t *testing.T) {
			b := &fakeBinding{values: map[string]any{addr: want}}
			engine := newTestEngine(t, b, func(c *Config) {
				c.Script = fmt.Sprintf(`
					const v = opcua.read({nodeId: %q});
					if (v !== %s) { return opcua.error_persistent("changed: " + v); }
					return opcua.success({saw: v});
				`, addr, jsLiteral(want))
			})
			res := engine.Run(t.Context(), map[string]any{})
			if res.Outcome != Delivered {
				t.Fatalf("value did not pass through unchanged: outcome %v, err %v",
					res.Outcome, res.Err)
			}
		})
	}
}

// jsLiteral renders a Go value the way the script wrote it.
func jsLiteral(v any) string {
	switch v := v.(type) {
	case bool:
		return fmt.Sprintf("%v", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%v", v)
	case string:
		return fmt.Sprintf("%q", v)
	}
	return "undefined"
}

// The engine offers call_method only when the binding can call (BND-7's
// optional half), and a callable address carries its object and its method in
// one object the script passes whole.
func TestRun_OffersCallMethodOnlyWhenTheBindingCanCall(t *testing.T) {
	t.Run("offered when the binding can call", func(t *testing.T) {
		b := &fakeCaller{fakeBinding: fakeBinding{}, outputs: []any{int64(7)}}
		engine := newTestEngine(t, b, func(c *Config) {
			c.Script = `
				const out = opcua.call_method({nodeId: "ns=2;s=Method", objectId: "ns=2;s=Object"}, [1, 2]);
				if (out[0] !== 7) { return opcua.error_persistent("wrong outputs: " + out); }
				return opcua.success({called: true});
			`
		})
		res := engine.Run(t.Context(), map[string]any{})
		if res.Outcome != Delivered {
			t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
		}
		if n := b.count("call ns=2;s=Method([1 2])"); n != 1 {
			t.Fatalf("method called %d times, want 1", n)
		}
	})

	t.Run("absent when the binding cannot", func(t *testing.T) {
		b := &fakeBinding{}
		engine := newTestEngine(t, b, func(c *Config) {
			c.Script = `return opcua.success({});`
		})
		// Assert the absence, not an outcome: the mutation pass registered
		// call_method unconditionally but made it refuse persistently, and
		// the old assertions stayed green, because a wrong script and a
		// refused method look alike from the result.
		ns := engine.namespace(&runState{}, true)
		if _, offered := ns["call_method"]; offered {
			t.Fatalf("call_method was offered to a binding that cannot call")
		}
		for _, want := range []string{"read", "write", "wait_until", "success", "error_not_ready", "error_transient", "error_persistent"} {
			if _, ok := ns[want]; !ok {
				t.Fatalf("%s missing from the namespace", want)
			}
		}
	})
}

// The seam to pkg/js_runtime is an interface, so a fake can stand in where a
// real runtime would be slow. This is that fake, proving the injection works.
type fakeRuntime struct {
	compileErr error
	runErr     error
}

func (f fakeRuntime) compile(name string, _ string) (any, error) {
	if f.compileErr != nil {
		return nil, fmt.Errorf("%s: %w", name, f.compileErr)
	}
	return &fakeProgram{name: name}, nil
}

func (f fakeRuntime) run(_ context.Context, _ any, _ map[string]any) (any, error) {
	if f.runErr != nil {
		return nil, f.runErr
	}
	return nil, nil
}

func (f fakeRuntime) close() error { return nil }

type fakeProgram struct{ name string }

// A script fault a user reads must be the fault they wrote, not a
// placeholder. The mutation pass replaced wrapRunError's return with a
// fixed errors.New("script fault") and all 23 tests stayed green, because
// the only test feeding the runErr→Result.Err link used a mock error.
// This one throws a real exception through the real runtime and demands
// its own text back.
func TestRun_AScriptFaultReachesTheResultUnmangled(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `throw new Error("distinctive script fault 9001");`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	if !strings.Contains(res.Err.Error(), "distinctive script fault 9001") {
		t.Fatalf("the script's own text did not reach Result.Err: %v", res.Err)
	}
}

// The real stand-in runtime must stop a running script when its context
// is done. The mutation pass found nothing pinned this: the fake-runtime
// test above exercises the fake, and the engine tests reach the runtime
// only indirectly. This drives the seam itself.
func TestRuntime_TheRealRuntimeStopsWhenItsContextIsDone(t *testing.T) {
	rt := newRuntime()
	defer func() { _ = rt.close() }()
	program, err := rt.compile("spin", `for (;;) {}`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, runErr := rt.run(ctx, program, nil)
	if runErr == nil || !errors.Is(runErr, context.DeadlineExceeded) {
		t.Fatalf("a run under an expired context must stop with ctx's own error, got %v", runErr)
	}
	if wall := time.Since(start); wall > 2*time.Second {
		t.Fatalf("the runtime did not stop when ctx was done: %v", wall)
	}
}

// A fake runtime stands in the seam: the engine drives it through the same
// interface the real package will satisfy.
func TestRuntime_FakeStandsInTheSeam(t *testing.T) {
	restore := newRuntime
	newRuntime = func() scriptRuntime {
		return fakeRuntime{runErr: fmt.Errorf("stopped: %w", context.Canceled)}
	}
	defer func() { newRuntime = restore }()

	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `return opcua.success({});`
	})
	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	if !errors.Is(res.Err, context.Canceled) {
		t.Fatalf("Err does not wrap the runtime's error: %v", res.Err)
	}
}

// A json.Number that is a number to neither Go parser — 1e999 overflows
// Float64 and is not integer syntax for Int64, and encoding/json with
// UseNumber preserves such a literal from the wire — crosses to the script
// as its own text rather than as a broken number. The script's author sees
// exactly what the message carried.
func TestRun_AJSONNumberThatIsNoNumberCrossesAsItsOwnText(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `
			if (typeof msg.x !== "string" || msg.x !== "1e999") {
				return opcua.error_persistent("msg.x is a " + typeof msg.x + ": " + msg.x);
			}
			return opcua.success({saw: msg.x});
		`
	})

	res := engine.Run(t.Context(), map[string]any{"x": json.Number("1e999")})
	if res.Outcome != Delivered {
		t.Fatalf("outcome %v, err %v: an unparsable json.Number must cross as its text", res.Outcome, res.Err)
	}
	d, ok := res.Data.(map[string]any)
	if !ok || d["saw"] != "1e999" {
		t.Fatalf("Data is %#v, want the number's own text", res.Data)
	}
}

// read's entry owns its argument conversion, so a bad address argument is a
// TypeError with goja's own wording — the text a script that catches it
// always read — and the machine is never asked for anything.
func TestRead_ANonObjectAddressIsATypeErrorTheScriptCanCatch(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `
			try {
				opcua.read(7);
			} catch (e) {
				return opcua.success({caught: String(e)});
			}
			return opcua.error_transient("not caught");
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Delivered {
		t.Fatalf("outcome %v, err %v: the TypeError must be catchable", res.Outcome, res.Err)
	}
	d, ok := res.Data.(map[string]any)
	if !ok || !strings.Contains(fmt.Sprint(d["caught"]), "could not convert function call parameter 0") {
		t.Fatalf("the conversion failure must keep goja's own wording, got %v", d["caught"])
	}
	if ops := b.snapshot(); len(ops) != 0 {
		t.Fatalf("the machine was asked for something (ops: %v)", ops)
	}
}

// readOp records a refused parse as the run's own fault (LIB-5): what a
// binding marks is what Run reports, and the fault wraps the binding's
// error so errors.Is still works.
func TestRead_AnAddressThatDoesNotParseIsTheRunsOwnFault(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `opcua.read({});` // no nodeId: ParseAddress refuses, marked persistent
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v: a parse refusal the binding marked persistent is persistent", res.Outcome, res.Err)
	}
	if !strings.Contains(res.Err.Error(), "parse address failed") {
		t.Fatalf("Err must name the refused parse, got %v", res.Err)
	}
	if !errors.Is(res.Err, ErrPersistent) {
		t.Fatalf("Err must wrap the binding's marked error, got %v", res.Err)
	}
}

// wait_until's predicate argument has the same rule as every other argument:
// a value that is not a function is a conversion failure with goja's own
// wording, not the missing-predicate error — a wrong script either way, but
// the script's author reads the text they always read.
func TestWaitUntil_APredicateThatIsNotAFunctionIsATypeError(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Loaded": false}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `
			try {
				opcua.wait_until({nodeId: "Loaded"}, 5, 10);
			} catch (e) {
				return opcua.success({caught: String(e)});
			}
			return opcua.error_transient("not caught");
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Delivered {
		t.Fatalf("outcome %v, err %v: the TypeError must be catchable", res.Outcome, res.Err)
	}
	d, ok := res.Data.(map[string]any)
	if !ok || !strings.Contains(fmt.Sprint(d["caught"]), "could not convert function call parameter 1") {
		t.Fatalf("the conversion failure must keep goja's own wording, got %v", d["caught"])
	}
	if ops := b.snapshot(); len(ops) != 0 {
		t.Fatalf("the machine was asked for something (ops: %v)", ops)
	}
}

// call_method's entries own their conversions, so a bad argument is evidence
// at the dispatch boundary (door 8): a cleanup block that catches the throw
// has still not handed the machine back (LIB-4). The machine is never asked.
func TestCallMethod_ANonObjectArgumentIsEvidenceTheBoundaryRecords(t *testing.T) {
	caller := &fakeCaller{fakeBinding: fakeBinding{values: map[string]any{}}}
	engine := newTestEngine(t, caller, func(c *Config) {
		c.Script = `return opcua.error_transient("machine busy");`
		c.OnError = `try { opcua.call_method(42, []); } catch (err) { }`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v: a swallowed conversion failure still means the machine was not handed back",
			res.Outcome, res.Err)
	}
	if res.CleanupErr == nil {
		t.Fatalf("the boundary's evidence is not reported in CleanupErr")
	}
	if !strings.Contains(res.CleanupErr.Error(), "call_method") ||
		!strings.Contains(res.CleanupErr.Error(), "could not convert") {
		t.Fatalf("the cleanup failure must name call_method and the conversion, got %v", res.CleanupErr)
	}
	if ops := caller.snapshot(); len(ops) != 0 {
		t.Fatalf("the machine was asked for something (ops: %v)", ops)
	}
}

// call_method's inputs argument may be omitted: a method with no inputs is
// called with none, not refused — a missing argument converts to nil, and
// ParseAddress-style refusal belongs to the binding's Call, not the entry.
func TestCallMethod_AnOmittedInputsArgumentMeansNoInputs(t *testing.T) {
	caller := &fakeCaller{fakeBinding: fakeBinding{values: map[string]any{}}, outputs: []any{int64(1)}}
	engine := newTestEngine(t, caller, func(c *Config) {
		c.Script = `
			const out = opcua.call_method({nodeId: "Reset"});
			if (out[0] !== 1) { return opcua.error_persistent("wrong outputs: " + out); }
			return opcua.success({called: true});
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Delivered {
		t.Fatalf("outcome %v, err %v: a method with no inputs must be callable", res.Outcome, res.Err)
	}
	if n := caller.count("call Reset([])"); n != 1 {
		t.Fatalf("the method was called %d times, want 1 (ops: %v)", n, caller.snapshot())
	}
	if caller.lastCall != nil {
		t.Fatalf("the binding received inputs %v, want none", caller.lastCall)
	}
}

// The other half of the inputs conversion: an argument that is present but
// not an array is a conversion failure, and in a cleanup block it is the
// boundary's evidence — caught or not, the block did not finish.
func TestCallMethod_ANonArrayInputsArgumentIsEvidenceTheBoundaryRecords(t *testing.T) {
	caller := &fakeCaller{fakeBinding: fakeBinding{values: map[string]any{}}}
	engine := newTestEngine(t, caller, func(c *Config) {
		c.Script = `return opcua.error_transient("machine busy");`
		c.OnError = `try { opcua.call_method({nodeId: "Reset"}, 5); } catch (err) { }`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v: a swallowed conversion failure still means the machine was not handed back",
			res.Outcome, res.Err)
	}
	if res.CleanupErr == nil ||
		!strings.Contains(res.CleanupErr.Error(), "could not convert function call parameter 1") {
		t.Fatalf("the cleanup failure must name the conversion, got %v", res.CleanupErr)
	}
	if ops := caller.snapshot(); len(ops) != 0 {
		t.Fatalf("the machine was asked for something (ops: %v)", ops)
	}
}

// UAT-13's rule on the call seam: a call_method under a transaction that has
// already been decided is refused before the machine is asked — the first
// ending decided the transaction, and nothing may extend it.
func TestCallMethod_ACallUnderAnEndedTransactionIsRefused(t *testing.T) {
	caller := &fakeCaller{fakeBinding: fakeBinding{values: map[string]any{}}, outputs: []any{}}
	engine := newTestEngine(t, caller, func(c *Config) {
		c.Script = `
			try { return opcua.success({done: true}); } catch (e) { }
			try { opcua.call_method({nodeId: "Reset"}); } catch (e) { }
			return opcua.error_transient("unreachable in practice");
		`
	})

	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Delivered {
		t.Fatalf("outcome %v, err %v: the first ending wins", res.Outcome, res.Err)
	}
	d, ok := res.Data.(map[string]any)
	if !ok || d["done"] != true {
		t.Fatalf("Data is %#v, want the first ending's object", res.Data)
	}
	if n := caller.count("call Reset([])"); n != 0 {
		t.Fatalf("the machine was called under a transaction that had already ended (ops: %v)", caller.snapshot())
	}
}
