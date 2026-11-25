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

package js_runtime

import (
	"context"
	"errors"
	"math"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dop251/goja"
)

func mustCompile(t *testing.T, name, source string) *Program {
	t.Helper()
	p, err := Compile(name, source)
	if err != nil {
		t.Fatalf("Compile(%q): %v", source, err)
	}
	return p
}

func run(t *testing.T, r *Runner, ctx context.Context, p *Program, globals map[string]any) any {
	t.Helper()
	v, err := r.Run(ctx, p, globals)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return v
}

func toFloat(t *testing.T, v any) float64 {
	t.Helper()
	switch n := v.(type) {
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case uint:
		return float64(n)
	case uint32:
		return float64(n)
	case uint64:
		return float64(n)
	case float32:
		return float64(n)
	case float64:
		return n
	default:
		t.Fatalf("not a number: %T (%v)", v, v)
		return math.NaN()
	}
}

// goroutineID returns the numeric id of the calling goroutine, taken from its
// stack header. Used to prove that Run executed inline (no goroutine spawn).
func goroutineID() string {
	buf := make([]byte, 1024)
	n := runtime.Stack(buf, false)
	header := strings.SplitN(string(buf[:n]), "\n", 2)[0]
	fields := strings.Fields(header)
	if len(fields) < 2 {
		return ""
	}
	return fields[1]
}

// TestCompileWrapAllowsTopLevelReturn pins the wrap Compile applies: a script
// with a top-level `return` compiles, and running it yields the return value.
func TestCompileWrapAllowsTopLevelReturn(t *testing.T) {
	r := NewRunner(Options{})
	p := mustCompile(t, "wrap", "return 42")

	v := run(t, r, context.Background(), p, nil)
	if got := toFloat(t, v); got != 42 {
		t.Fatalf("wrapped return: got %v, want 42", got)
	}
}

// TestGojaRejectsTopLevelReturnUnwrapped is the control for the wrap test: the
// same source without the wrap does not compile in goja.
func TestGojaRejectsTopLevelReturnUnwrapped(t *testing.T) {
	_, err := goja.Compile("wrap-control", "return 42", false)
	if err == nil {
		t.Fatal("unwrapped top-level return compiled, expected goja to reject it")
	}
}

// TestRunBindsGlobalsAndClearsThemAfterRun pins the "reset only the message"
// semantics: globals bound by a run are gone again before the next run.
func TestRunBindsGlobalsAndClearsThemAfterRun(t *testing.T) {
	r := NewRunner(Options{})
	add := mustCompile(t, "add", "return a + b")
	probe := mustCompile(t, "probe", "return typeof a")

	v := run(t, r, context.Background(), add, map[string]any{"a": 2, "b": 40})
	if got := toFloat(t, v); got != 42 {
		t.Fatalf("a + b: got %v, want 42", got)
	}

	// The bound globals of the previous run must not survive into this one.
	v = run(t, r, context.Background(), probe, nil)
	if v != "undefined" {
		t.Fatalf("typeof a after reset: got %v, want \"undefined\"", v)
	}
}

// TestZeroDeadlineRunsInline pins that a Run with no deadline executes on the
// caller's goroutine rather than spawning one. This is the no-time-limit path
// nodered_js and tag_processor use on every message.
func TestZeroDeadlineRunsInline(t *testing.T) {
	r := NewRunner(Options{})
	p := mustCompile(t, "goid", "return capture()")

	var ranOn string
	globals := map[string]any{
		"capture": func() string {
			ranOn = goroutineID()
			return ranOn
		},
	}

	caller := goroutineID()
	v := run(t, r, context.Background(), p, globals)
	if v != caller {
		t.Fatalf("script ran on goroutine %v, want the caller's goroutine %v", v, caller)
	}
	if ranOn != caller {
		t.Fatalf("native callback ran on goroutine %v, want %v", ranOn, caller)
	}
}

// TestDeadlineInterruptsAndSurfacesCallerError pins the watchdog: a run whose
// context deadline expires is interrupted, returns promptly, and surfaces the
// caller's own context error (not a goja interrupt message). It also checks
// the interrupted runtime stopped and is reusable.
func TestDeadlineInterruptsAndSurfacesCallerError(t *testing.T) {
	r := NewRunner(Options{})
	p := mustCompile(t, "loop", "while(true) {}")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := r.Run(ctx, p, nil)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("infinite loop with 200ms deadline returned no error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error does not wrap context.DeadlineExceeded: %v", err)
	}
	if !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("error does not surface the caller's error text: %v", err)
	}
	// 200ms deadline + at most the 1s interrupt grace.
	if elapsed > 2*time.Second {
		t.Fatalf("Run took %v, want it back within 2s", elapsed)
	}

	// The script must actually have stopped: the next run succeeds quickly.
	answer := mustCompile(t, "answer", "return 6 * 7")
	start = time.Now()
	v := run(t, r, context.Background(), answer, nil)
	if got := toFloat(t, v); got != 42 {
		t.Fatalf("run after interrupt: got %v, want 42", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("run after interrupt took %v, want < 1s", elapsed)
	}
}

// TestResetFailureDiscardsRuntime pins pool hygiene: if cleaning a runtime
// after a run fails, the runtime is discarded instead of pooled. A script can
// make the reset fail by freezing the global object.
func TestResetFailureDiscardsRuntime(t *testing.T) {
	r := NewRunner(Options{})
	freeze := mustCompile(t, "freeze", `Object.freeze(globalThis); return "frozen"`)
	probe := mustCompile(t, "probe", "return Object.isFrozen(globalThis)")
	globals := map[string]any{"msg": map[string]any{"payload": 1}}

	v := run(t, r, context.Background(), freeze, globals)
	if v != "frozen" {
		t.Fatalf("freeze script: got %v, want \"frozen\"", v)
	}

	// If the frozen runtime were pooled, binding msg on it would fail
	// (the object is not extensible) and this run would error.
	v = run(t, r, context.Background(), probe, globals)
	if frozen, ok := v.(bool); !ok || frozen {
		t.Fatalf("pooled runtime carried the frozen global object: got %v, want false", v)
	}
}

// TestResetGlobalsClearsScriptGlobals pins the ResetGlobals option: globals a
// script created do not survive to the next run.
func TestResetGlobalsClearsScriptGlobals(t *testing.T) {
	r := NewRunner(Options{ResetGlobals: true})
	set := mustCompile(t, "set", `globalThis.marker = 42; return "set"`)
	probe := mustCompile(t, "probe", `return typeof globalThis.marker`)

	run(t, r, context.Background(), set, nil)
	v := run(t, r, context.Background(), probe, nil)
	if v != "undefined" {
		t.Fatalf("ResetGlobals: typeof marker = %v, want \"undefined\"", v)
	}
}

// TestGlobalsSurviveWithoutResetGlobals is the control for ResetGlobals and
// pins today's behaviour for callers that do not opt in: a global a script
// created survives into the next run on the same pooled runtime.
func TestGlobalsSurviveWithoutResetGlobals(t *testing.T) {
	r := NewRunner(Options{})
	set := mustCompile(t, "set", `globalThis.marker = 42; return "set"`)
	probe := mustCompile(t, "probe", `return typeof globalThis.marker`)

	run(t, r, context.Background(), set, nil)
	v := run(t, r, context.Background(), probe, nil)
	if v != "number" {
		t.Fatalf("no ResetGlobals: typeof marker = %v, want \"number\"", v)
	}
}

// TestOnSlow pins the slow-run callback: a run longer than Slow is reported
// with its duration, a fast run is not.
func TestOnSlow(t *testing.T) {
	var got time.Duration
	called := false
	r := NewRunner(Options{
		Slow:   time.Millisecond,
		OnSlow: func(d time.Duration) { got, called = d, true },
	})

	fast := mustCompile(t, "fast", "return 1")
	run(t, r, context.Background(), fast, nil)
	if called {
		t.Fatal("OnSlow called for a sub-millisecond run")
	}

	slow := mustCompile(t, "slow", `var t = Date.now(); while (Date.now() - t < 50); return t`)
	run(t, r, context.Background(), slow, nil)
	if !called {
		t.Fatal("OnSlow not called for a ~50ms run")
	}
	if got < 40*time.Millisecond {
		t.Fatalf("OnSlow duration %v, want >= 40ms", got)
	}
}

// TestClose makes sure Close is callable and succeeds (goja runtimes are
// garbage collected; there is nothing to release).
func TestClose(t *testing.T) {
	r := NewRunner(Options{})
	p := mustCompile(t, "close", "return 1")
	run(t, r, context.Background(), p, nil)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
