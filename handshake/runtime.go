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
	"time"

	"github.com/dop251/goja"
)

// compile applies the function wrap every caller in this repo already uses,
// (function(){ 'use strict'; ... })(), so a script may use a top-level return.
//
// The prefix is three lines so a user's first line is line 4, the same as
// pkg/js_runtime. That number is not cosmetic: a handshake script that
// throws reaches the flow as "handshake ended persistent: Error: boom at
// script:4:7", so it is in the dead-letter reason and in the operator's
// logs, and it is how someone finds the fault in a long script.
//
// It matters most for what has not happened yet. When this stand-in is
// swapped for pkg/js_runtime, a different prefix length would move every
// line number a customer has ever seen, silently, with nothing failing.
// Matching now means the swap changes nothing. TestCompile_AUsersFirstLine
// goes red if either side drifts.
func compile(name string, source string) (*goja.Program, error) {
	wrapped := "\n\n(function(){'use strict';\n" + source + "\n})()"
	program, err := goja.Compile(name, wrapped, false)
	if err != nil {
		return nil, fmt.Errorf("%s does not compile: %w", name, err)
	}
	return program, nil
}

// runProgram binds globals into a fresh goja runtime and executes prog. A
// fresh runtime per run is the strict reset LIB-7 requires. The deadline
// comes from ctx: the watcher goroutine interrupts the runtime when ctx is
// done, and the returned error wraps ctx.Err(). The runtime is discarded
// after the run, so an interrupt that arrives late cannot hurt the next one.
func runProgram(ctx context.Context, prog *goja.Program, globals map[string]any) (result any, err error) { //nolint:nonamedreturns // a deferred recover assigns to it; naming is the mechanism
	// No panic from a script or a binding leaves Run (round-2 review,
	// item 2): goja fills a missing function argument with a nil func, a
	// binding is protocol code that can blow up on a machine's bad answer,
	// and in Benthos an escaped panic is the process — with the machine
	// left armed and neither cleanup run. A recovered panic is a script
	// fault: persistent, and the cleanup pair still runs, because Run
	// reads the error like any other.
	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = fmt.Errorf("the script or a binding panicked: %v", r)
		}
	}()
	vm := goja.New()
	for name, value := range globals {
		if bindErr := vm.Set(name, jsValue(value)); bindErr != nil {
			return nil, fmt.Errorf("cannot bind global %q: %w", name, bindErr)
		}
	}
	stop := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-ctx.Done():
			vm.Interrupt(ctx.Err())
		case <-stop:
		}
	}()
	// Deferred, so a binding that panics through RunProgram stops the
	// watcher too; otherwise it waits for a ctx that, in an unbudgeted
	// cleanup, is never done. It runs before the recover above.
	defer func() {
		close(stop)
		<-watchDone
	}()
	value, err := vm.RunProgram(prog)
	if err != nil {
		return nil, wrapRunError(err)
	}
	return value.Export(), nil
}

// jsValue prepares one Go value for the boundary into the script. A JSON
// number reaches the engine as json.Number - the type a structured message
// carries - and goja would bind that string-kind type as a string, so the
// script would see msg.setpoint === 1.5 fail on a message that arrived as
// JSON. A number is a number in the script, whatever Go type carried it;
// everything else crosses as it is. Maps and slices are copied, so the
// message the caller handed over is not written to.
func jsValue(v any) any {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return t.String()
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = jsValue(e)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = jsValue(e)
		}
		return out
	default:
		return v
	}
}

// wrapRunError keeps an interrupt recognizable as ctx's own error, by
// leaving the chain intact; every other error is a script fault for the
// engine to classify.
func wrapRunError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("script was stopped before it ended: %w", err)
	}
	return err
}

// namespace builds the object a script calls its machine through, under the
// binding's own word for itself (Namespace, and from nowhere else). Every
// entry is a goja native function — func(FunctionCall, *Runtime) Value —
// so the entry, not goja's reflective wrapper, owns what happens at the
// dispatch boundary. Endings are offered to the script only: a cleanup
// block returns the machine to idle and has nothing to end.
//
// This is the dispatch boundary where a cleanup block's operations are
// dispatched, so it is where the cleanup's evidence is recorded
// (cleanupEvidence): every operation a block can ask for goes through one
// of these entries, and a failure surfacing at one of them means the
// block's job — handing the machine back — did not finish, whatever line
// inside the operation produced it. Three mechanisms carry a failure to
// this boundary, and the boundary witnesses all three:
//
//   - an operation that RETURNS an error (a refused write, a wait that
//     ran out): the entry records it before throwing it to the script;
//   - an operation whose callback PANICS out of the Go frame (door 7: the
//     wait's predicate throws, and goja unwinds the entry's frame by
//     panic, so nothing returns): the entry recovers it, records it, and
//     re-raises the identical value, so the script sees exactly the
//     exception it saw before — a record is added, control flow is not
//     changed (recoverEvidence);
//   - an argument CONVERSION that fails before the Go body runs (door 8:
//     goja raised the TypeError above the entry, so the boundary never
//     saw it): the entry owns its conversion, so a bad argument is an
//     error the entry records and then throws, with goja's own message
//     text, rather than a type error raised above it (argObject and
//     friends).
//
// read is the one exempt entry: it cannot change the machine, so a
// cleanup may probe, catch the refusal, and carry on. A read inside
// wait_until is not exempt, because that error surfaces at the wait's
// entry, not read's.
func (e *Engine[UnifiedAddress]) namespace(rs *runState, endings bool) map[string]any {
	ns := map[string]any{
		"read": func(call goja.FunctionCall, vm *goja.Runtime) goja.Value {
			// No cleanupEvidence: a read cannot change the machine, so its
			// refusal is not the cleanup's failure (see cleanupEvidence for
			// the wait's other half). The entry still owns its conversion,
			// so a bad argument throws the same TypeError it always threw;
			// goja raised this one above the boundary too — it just never
			// counted as evidence, and at read's exempt entry it still does
			// not.
			raw, convErr := argObject(call, 0)
			if convErr != nil {
				panic(vm.NewTypeError("%s", convErr.Error()))
			}
			v, err := e.readOp(rs, raw)
			if err != nil {
				panic(vm.NewGoError(err))
			}
			return vm.ToValue(v)
		},
		"write": func(call goja.FunctionCall, vm *goja.Runtime) (ret goja.Value) { //nolint:nonamedreturns // a deferred recover assigns to it; naming is the mechanism
			own := false
			defer rs.recoverEvidence("write", &own)
			raw, convErr := argObject(call, 0)
			if convErr != nil {
				rs.cleanupEvidence("write", convErr)
				own = true
				panic(vm.NewTypeError("%s", convErr.Error()))
			}
			err := e.writeOp(rs, raw, call.Argument(1).Export())
			rs.cleanupEvidence("write", err)
			if err != nil {
				own = true
				panic(vm.NewGoError(err))
			}
			return goja.Undefined()
		},
		"wait_until": func(call goja.FunctionCall, vm *goja.Runtime) (ret goja.Value) { //nolint:nonamedreturns // a deferred recover assigns to it; naming is the mechanism
			own := false
			defer rs.recoverEvidence("wait_until", &own)
			raw, convErr := argObject(call, 0)
			var pred func(any) bool
			if convErr == nil {
				pred, convErr = argPredicate(vm, call.Argument(1))
			}
			var timeoutMs float64
			if convErr == nil {
				timeoutMs, convErr = argNumber(call, 2)
			}
			var opts map[string]any
			if convErr == nil {
				opts, convErr = argObject(call, 3)
			}
			if convErr != nil {
				rs.cleanupEvidence("wait_until", convErr)
				own = true
				panic(vm.NewTypeError("%s", convErr.Error()))
			}
			v, err := e.waitUntil(rs, raw, pred, timeoutMs, opts)
			rs.cleanupEvidence("wait_until", err)
			if err != nil {
				own = true
				panic(vm.NewGoError(err))
			}
			return vm.ToValue(v)
		},
	}
	if caller, ok := e.bind.(Caller[UnifiedAddress]); ok {
		ns["call_method"] = func(call goja.FunctionCall, vm *goja.Runtime) (ret goja.Value) { //nolint:nonamedreturns // a deferred recover assigns to it; naming is the mechanism
			own := false
			defer rs.recoverEvidence("call_method", &own)
			raw, convErr := argObject(call, 0)
			var inputs []any
			if convErr == nil {
				inputs, convErr = argInputs(call, 1)
			}
			if convErr != nil {
				rs.cleanupEvidence("call_method", convErr)
				own = true
				panic(vm.NewTypeError("%s", convErr.Error()))
			}
			v, err := e.callOp(rs, caller, raw, inputs)
			rs.cleanupEvidence("call_method", err)
			if err != nil {
				own = true
				panic(vm.NewGoError(err))
			}
			return vm.ToValue(v)
		}
	}
	if endings {
		ns["success"] = func(call goja.FunctionCall, vm *goja.Runtime) goja.Value {
			data := call.Argument(0).Export()
			// The data leaves the engine in Result.Data. Data that does not
			// encode -- an object that contains itself, a NaN -- is refused
			// here as a script mistake rather than handed to a caller whose
			// logging or encoding would choke on it.
			if _, err := json.Marshal(data); err != nil {
				panic(vm.NewTypeError("success() was given data that cannot be encoded as JSON: %v", err))
			}
			if rs.finish(endSuccess, nil) {
				rs.data = data
			}
			panic(vm.NewGoError(errEnded))
		}
		ns["error_not_ready"] = func(_ goja.FunctionCall, vm *goja.Runtime) goja.Value {
			rs.finish(endNotReady, nil)
			panic(vm.NewGoError(errEnded))
		}
		ns["error_transient"] = func(call goja.FunctionCall, vm *goja.Runtime) goja.Value {
			rs.finish(endTransient, call.Argument(0).Export())
			panic(vm.NewGoError(errEnded))
		}
		ns["error_persistent"] = func(call goja.FunctionCall, vm *goja.Runtime) goja.Value {
			rs.finish(endPersistent, call.Argument(0).Export())
			panic(vm.NewGoError(errEnded))
		}
	}
	return ns
}

// recoverEvidence closes door 7 at the dispatch boundary (see the comment
// on namespace, above). A callback the script passed into an operation —
// today, wait_until's predicate — that throws does not return through the
// entry: goja unwinds the Go frame by panic, so the code after the
// operation never runs. The entry defers this, so the boundary recovers
// the panic, records it as the cleanup's evidence, and re-raises the
// identical value: the script sees exactly the exception it saw before
// the boundary learned to look, caught or not. An interrupt and a Go
// runtime fault pass through here the same way — recorded, then raised
// untouched — so nothing changes control flow.
//
// own marks the throws the entry raised itself, whose evidence the entry
// already recorded before throwing, so that a failure is recorded exactly
// once, whichever of the three mechanisms carried it. This is a method on
// runState rather than a closure in each entry so the recover sits in one
// auditable place: recover only sees a panic when the deferred function
// calls it itself.
func (rs *runState) recoverEvidence(op string, own *bool) {
	x := recover()
	if x == nil {
		return
	}
	if !*own {
		rs.cleanupEvidence(op, panicAsError(x))
	}
	panic(x) // not a change of control flow: the identical value unwinds on
}

// panicAsError turns a recovered panic into the error the boundary
// records. A JavaScript throw arrives as goja's own *Exception (Error()
// renders the script's message); an interrupt or a Go runtime fault is an
// error already. The wrapper is a record of what unwound, never a
// replacement for it: the original value is re-raised untouched.
func panicAsError(x any) error {
	switch x1 := x.(type) {
	case *goja.Exception:
		return fmt.Errorf("the callback threw: %w", x1)
	case error:
		return x1
	default:
		return fmt.Errorf("the operation panicked: %v", x1)
	}
}

// argObject converts argument i of a call into the address object a script
// passes whole. The entry owns this conversion (door 8): when goja's
// reflective wrapper did it, a bad argument raised a TypeError above the
// entry, before the Go body ran, so the boundary never saw the failure;
// owned, the conversion is an error the entry records like any other. The
// message keeps goja's own wording, so a script that catches the throw
// reads the text it always read. A missing or null argument converts to a
// nil address, as the reflective binding's zero value did, so ParseAddress
// still owns that refusal.
func argObject(call goja.FunctionCall, i int) (map[string]any, error) {
	v := call.Argument(i)
	if v == goja.Undefined() || v == goja.Null() {
		return nil, nil
	}
	m, ok := v.Export().(map[string]any)
	if !ok {
		return nil, fmt.Errorf(
			"could not convert function call parameter %d: could not convert %v to map[string]interface {}", i, v)
	}
	return m, nil
}

// argInputs converts call_method's second argument into the method's input
// array; see argObject for why the entry owns the conversion.
func argInputs(call goja.FunctionCall, i int) ([]any, error) {
	v := call.Argument(i)
	if v == goja.Undefined() || v == goja.Null() {
		return nil, nil
	}
	s, ok := v.Export().([]any)
	if !ok {
		return nil, fmt.Errorf(
			"could not convert function call parameter %d: could not convert %v to []interface {}", i, v)
	}
	return s, nil
}

// argNumber converts a call's numeric argument; see argObject for why the
// entry owns the conversion. A JS number exports as int64 or float64,
// whichever it fits, and both convert, as the reflective binding's
// conversion did.
func argNumber(call goja.FunctionCall, i int) (float64, error) {
	v := call.Argument(i)
	if v == goja.Undefined() || v == goja.Null() {
		return 0, nil
	}
	if n, ok := number(v.Export()); ok {
		return n, nil
	}
	// goja's reflective binding used ToFloat, which accepts a numeric
	// string and a boolean. A timeout read out of a message often arrives
	// as a string, so refusing one here would break a working script.
	if f := v.ToFloat(); !math.IsNaN(f) {
		return f, nil
	}
	return 0, fmt.Errorf(
		"could not convert function call parameter %d: could not convert %v to float64", i, v)
}

// argPredicate converts wait_until's second argument into the Go callback
// the wait polls with; see argObject for why the entry owns the conversion.
// A missing or null predicate stays nil, so the wait's own friendly error
// owns that wrong script (door 6, unchanged); a predicate that is not a
// function fails the conversion like any other argument. The wrapper
// reproduces what the reflective binding's wrapper did: the value crosses
// as a JS value, the result is read with ToBoolean so a truthy non-boolean
// satisfies the wait as JavaScript says it should, and a throw unwinds this
// Go frame as the panic it always was, for the boundary to witness
// (recoverEvidence).
func argPredicate(vm *goja.Runtime, v goja.Value) (func(any) bool, error) {
	if v == goja.Undefined() || v == goja.Null() {
		return nil, nil
	}
	fn, ok := goja.AssertFunction(v)
	if !ok {
		return nil, fmt.Errorf(
			"could not convert function call parameter 1: could not convert %v to func(interface {}) bool", v)
	}
	return func(arg any) bool {
		res, err := fn(goja.Undefined(), vm.ToValue(arg))
		if err != nil {
			panic(err) // the callback's throw, unwinding exactly as before
		}
		// goja's reflective binding converted with ToBoolean, so a truthy
		// non-boolean -- v.length, 1, "ok" -- satisfied the wait. Keep that:
		// a Go type assertion would silently read every one of them as false.
		b := res.ToBoolean()
		return b
	}, nil
}

// readOp runs one read and records the fault if it failed. A parse failure is
// recorded like a read failure: what a binding marks is what Run reports.
// After the run has ended the machine is not touched again: the ending
// functions abort the script by throwing errEnded, and a throw is catchable,
// so the operations themselves refuse too (UAT-13).
// readWithin is readOp bounded by the caller's own deadline rather than the
// run's. It reports whether the read was cut off by that bound while the run
// still had time, which is a wait running out rather than a machine fault --
// and so is NOT recorded as one.
func (e *Engine[UnifiedAddress]) readWithin(
	rs *runState, raw map[string]any, bound time.Time,
) (value any, cutOff bool, err error) { //nolint:nonamedreturns // three results, and the middle one is the question
	if rs.end != endNone {
		return nil, false, errEnded
	}
	if err := rs.ctx.Err(); err != nil {
		return nil, false, err // the deadline fired; the machine is not touched again
	}
	addr, err := e.bind.ParseAddress(raw)
	if err != nil {
		rs.fail("parse address", err)
		return nil, false, err
	}
	ctx, cancel := context.WithDeadline(rs.ctx, bound)
	defer cancel()
	resp, err := e.bind.Read(ctx, addr)
	if err != nil {
		// The wait's own bound expired and the run's did not: the machine
		// did not answer in the time the wait had, which is exactly what a
		// wait running out means. Recording it as a binding fault would
		// make the run persistent for a machine that was merely slow.
		if ctx.Err() != nil && rs.ctx.Err() == nil {
			return nil, true, err
		}
		rs.fail("read "+e.bind.Describe(addr), err)
		return nil, false, err
	}
	return resp.Value, false, nil
}

func (e *Engine[UnifiedAddress]) readOp(rs *runState, raw map[string]any) (any, error) {
	if rs.end != endNone {
		return nil, errEnded
	}
	if err := rs.ctx.Err(); err != nil {
		return nil, err // the deadline fired; the machine is not touched again
	}
	addr, err := e.bind.ParseAddress(raw)
	if err != nil {
		rs.fail("parse address", err)
		return nil, err
	}
	resp, err := e.bind.Read(rs.ctx, addr)
	if err != nil {
		rs.fail("read "+e.bind.Describe(addr), err)
		return nil, err
	}
	return resp.Value, nil
}

// writeOp sends one value, attempted once. A failure is recorded like any
// operation's, for the script's outcome; inside a cleanup block the
// dispatch boundary records what a refused write means (cleanupEvidence):
// a refused write leaves the machine's state unknown, which a cleanup
// block must not be able to catch its way around. After the run has ended
// the write is refused without reaching the machine (UAT-13): the first
// ending decided the transaction, and nothing may extend it.
func (e *Engine[UnifiedAddress]) writeOp(rs *runState, raw map[string]any, value any) error {
	if rs.end != endNone {
		return errEnded
	}
	if err := rs.ctx.Err(); err != nil {
		return err // the deadline fired; the machine is not touched again
	}
	addr, err := e.bind.ParseAddress(raw)
	if err != nil {
		rs.fail("parse address", err) // the write never reached the machine
		return err
	}
	if err := e.bind.Write(rs.ctx, addr, value); err != nil {
		rs.fail("write "+e.bind.Describe(addr), err)
		return err
	}
	return nil
}

// callOp invokes a callable address; a protocol without methods never reaches
// here, because the engine offers call_method only when the binding can call.
// A failure is recorded like any operation's, for the script's outcome; inside
// a cleanup block the dispatch boundary records what a refused call means
// (cleanupEvidence): a refused call leaves the machine's state unknown, like
// a write's.
func (e *Engine[UnifiedAddress]) callOp(
	rs *runState, caller Caller[UnifiedAddress], raw map[string]any, inputs []any,
) (any, error) {
	if rs.end != endNone {
		return nil, errEnded
	}
	if err := rs.ctx.Err(); err != nil {
		return nil, err // the deadline fired; the machine is not touched again
	}
	addr, err := e.bind.ParseAddress(raw)
	if err != nil {
		rs.fail("parse address", err) // the call never reached the machine
		return nil, err
	}
	resp, err := caller.Call(rs.ctx, addr, inputs)
	if err != nil {
		rs.fail("call "+e.bind.Describe(addr), err)
		return nil, err
	}
	return resp.Outputs, nil
}
