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
	"math"
	"runtime"
	"strings"
	"testing"
	"time"
)

// The script's own variables are reset for every run. A script's globals
// must not reach the next message: a handshake has no memory between
// messages.
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

// The value a binding returns reaches the script unchanged. The
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

// The engine offers call_method only when the binding implements Caller, and
// a callable address carries its object and its method in one object the
// script passes whole.
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
		// Assert the absence, not an outcome: a wrong script and a method
		// that always refuses look alike from the result.
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

// A script fault a user reads must be the fault they wrote, not a
// placeholder. This test throws a real exception through the real runtime
// and demands its own text back in Result.Err.
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

// The runtime must stop a running script when its context is done. The
// engine tests reach the runtime only indirectly; this drives it directly.
func TestRuntime_TheRealRuntimeStopsWhenItsContextIsDone(t *testing.T) {
	program, _, err := compile("spin", `for (;;) {}`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, runErr := runProgram(ctx, program, nil)
	if runErr == nil || !errors.Is(runErr, context.DeadlineExceeded) {
		t.Fatalf("a run under an expired context must stop with ctx's own error, got %v", runErr)
	}
	if wall := time.Since(start); wall > 2*time.Second {
		t.Fatalf("the runtime did not stop when ctx was done: %v", wall)
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

// read's entry converts its own argument, so a bad address argument is a
// TypeError with the wording of goja's reflective conversion, and the
// machine is never asked for anything.
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

// A refused parse is the run's own fault: what a binding marks is what Run
// reports, and the fault wraps the binding's error so errors.Is still works.
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
// a value that is not a function is a conversion failure with the wording of
// goja's reflective conversion, not the missing-predicate error. Both are a
// wrong script.
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

// call_method's entry converts its own arguments, so a bad argument is
// evidence at the dispatch boundary: a cleanup block that catches the throw
// has still not handed the machine back. The machine is never asked.
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
// called with none, not refused: a missing argument converts to nil, and
// refusing it is the binding's Call's decision, not the entry's.
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
// boundary's evidence: caught or not, the block did not finish.
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

// A call_method under a transaction that has already been decided is
// refused before the machine is asked: the first ending decided the
// transaction, and nothing may extend it.
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

// A binding that panics unwinds through RunProgram, and the watcher that
// interrupts the VM on ctx must still stop. Under a ctx that is never done,
// as an unbudgeted cleanup's is, a skipped stop leaks one goroutine per run.
func TestRuntime_APanickingBindingDoesNotLeakTheWatcher(t *testing.T) {
	program, _, err := compile("script", `boom();`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	globals := map[string]any{"boom": func() { panic("the binding blew up") }}

	before := runtime.NumGoroutine()
	for range 20 {
		if _, runErr := runProgram(context.Background(), program, globals); runErr == nil ||
			!strings.Contains(runErr.Error(), "panicked") {
			t.Fatalf("the panic must come back as an error, got %v", runErr)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines before 20 panicking runs, %d after: the watcher leaked",
				before, runtime.NumGoroutine())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Typed Go containers in a message cross as plain JavaScript arrays and
// objects, as a decoded JSON message would, nested ones included, and they
// are copies: a script that writes to msg does not write to the caller's map
// or slice.
func TestRun_TypedContainersInTheMessageCrossAsPlainCopies(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `
			if (!Array.isArray(msg.ints) || !Array.isArray(msg.nested.y) || !Array.isArray(msg.fixed)) {
				return opcua.error_persistent("a typed slice is not an array");
			}
			const seen = JSON.stringify([msg.ints, msg.counts, msg.nested, msg.fixed, msg.byID, msg.none]);
			msg.ints[0] = 99;
			msg.counts.x = 99;
			msg.nested.y[0] = 99;
			return opcua.success({seen: seen, sum: msg.ints.reduce((a, v) => a + v, 0)});
		`
	})

	ints := []int{1, 2}
	counts := map[string]int{"x": 1}
	nested := map[string][]float64{"y": {1.5}}
	res := engine.Run(t.Context(), map[string]any{
		"ints":   ints,
		"counts": counts,
		"nested": nested,
		"fixed":  [2]string{"p", "q"},
		"byID":   map[int]bool{7: true},
		"none":   []int(nil),
	})
	if res.Outcome != Delivered {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	d, ok := res.Data.(map[string]any)
	if !ok {
		t.Fatalf("Data is %#v", res.Data)
	}
	if want := `[[1,2],{"x":1},{"y":[1.5]},["p","q"],{"7":true},[]]`; d["seen"] != want {
		t.Errorf("the script saw %v, want %v", d["seen"], want)
	}
	if d["sum"] != int64(101) {
		t.Errorf("the script's own copy did not take its write: sum %#v", d["sum"])
	}
	if ints[0] != 1 || counts["x"] != 1 || nested["y"][0] != 1.5 {
		t.Fatalf("the script wrote to the caller's message: %v %v %v", ints, counts, nested)
	}
}

// A Go float that is not finite crosses as the JavaScript number it is, so a
// script can test it with Number.isFinite. A json.Number too large for any
// float is covered by TestRun_AJSONNumberThatIsNoNumberCrossesAsItsOwnText.
func TestRun_NonFiniteFloatsInTheMessageCrossAsJavaScriptNumbers(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `
			if (msg.inf !== Infinity || msg.ninf !== -Infinity || !Number.isNaN(msg.nan) || msg.small !== Infinity) {
				return opcua.error_persistent("got " + [msg.inf, msg.ninf, msg.nan, msg.small].join(","));
			}
			return opcua.success({finite: [msg.inf, msg.ninf, msg.nan].some(Number.isFinite)});
		`
	})
	res := engine.Run(t.Context(), map[string]any{
		"inf":   math.Inf(1),
		"ninf":  math.Inf(-1),
		"nan":   math.NaN(),
		"small": []float32{float32(math.Inf(1))}[0],
	})
	if res.Outcome != Delivered {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	if d, _ := res.Data.(map[string]any); d["finite"] != false {
		t.Fatalf("Data is %#v", res.Data)
	}

	// Returning one is refused: it cannot be encoded as JSON.
	engine = newTestEngine(t, b, func(c *Config) {
		c.Script = `return opcua.success({v: msg.inf});`
	})
	res = engine.Run(t.Context(), map[string]any{"inf": math.Inf(1)})
	if res.Outcome != Persistent || !strings.Contains(res.Err.Error(), "cannot be encoded") {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
}

// selfMap is a typed map, which jsValue copies by reflection rather than by
// its map[string]any case.
type selfMap map[string]any

// A message that contains itself cannot cross into the script: copying it
// would recurse until the Go stack overflows, which kills the process. The
// run is refused as a script-side mistake, Persistent and not a machine
// fault, before the script runs. The failure messages never format the
// message itself, which %v would also recurse on.
func TestRun_AMessageThatContainsItselfIsRefusedNotACrash(t *testing.T) {
	m := map[string]any{}
	m["self"] = m
	s := []any{nil}
	s[0] = s
	typed := selfMap{}
	typed["self"] = typed
	for _, tc := range []struct {
		name string
		msg  map[string]any
	}{
		{"a map holding itself", m},
		{"a slice holding itself", map[string]any{"list": s}},
		{"a typed map holding itself", map[string]any{"typed": typed}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &fakeBinding{values: map[string]any{"Ready": true}}
			engine := newTestEngine(t, b, func(c *Config) {
				c.Script = `opcua.read({nodeId: "Ready"}); return opcua.success({});`
			})
			res := engine.Run(t.Context(), tc.msg)
			if res.Outcome != Persistent {
				t.Fatalf("outcome %v, want Persistent (err %v)", res.Outcome, res.Err)
			}
			if res.Err == nil || !strings.Contains(res.Err.Error(), "contains itself") {
				t.Fatalf("Err does not say the message contains itself: %v", res.Err)
			}
			var f *fault
			if errors.As(res.Err, &f) {
				t.Fatalf("a message that cannot cross is not a machine fault: %v", res.Err)
			}
			if ops := b.snapshot(); len(ops) != 0 {
				t.Fatalf("the script ran on a message it cannot see: %v", ops)
			}
		})
	}
}
