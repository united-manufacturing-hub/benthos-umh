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
	"reflect"
	"strconv"
	"time"

	"github.com/dop251/goja"
	"github.com/dop251/goja/ast"
)

// compile wraps the source as (function(){'use strict'; ... })(), so a
// script may use a top-level return, and compiles it. It returns the parsed
// tree beside the program, so Validate reads the script's waits from the
// same parse.
//
// The prefix sits on the user's first line, so a line number in an error
// is the user's own line: a script that throws on its first line reaches the
// flow as "Error: boom at script:1:...", in the dead-letter reason and in
// the operator's logs. Only columns on that first line are shifted, by the
// prefix's length. The suffix starts on a new line, so a trailing // comment
// cannot swallow it. TestCompile_AUsersFirstLineIsLineOne pins this.
func compile(name string, source string) (*goja.Program, *ast.Program, error) {
	wrapped := "(function(){'use strict';" + source + "\n})()"
	tree, err := goja.Parse(name, wrapped)
	if err == nil {
		var program *goja.Program
		if program, err = goja.CompileAST(tree, false); err == nil {
			return program, tree, nil
		}
	}
	return nil, nil, fmt.Errorf("%s does not compile: %w", name, err)
}

// runProgram binds globals into a fresh goja runtime and executes prog, so
// nothing a script leaves behind reaches the next run. The deadline comes
// from ctx: the watcher goroutine interrupts the runtime when ctx is done,
// and the returned error wraps ctx.Err(). The runtime is discarded after
// the run, so an interrupt that arrives late cannot hurt the next one.
func runProgram(ctx context.Context, prog *goja.Program, globals map[string]any) (result any, err error) { //nolint:nonamedreturns // a deferred recover assigns to it; naming is the mechanism
	// No panic from a script or a binding leaves Run: a binding is protocol
	// code that can blow up on a machine's bad answer, and in Benthos an
	// escaped panic ends the process, with the machine left armed and
	// neither cleanup run. A recovered panic is a script fault: persistent,
	// and the cleanup still runs, because Run reads the error like any
	// other.
	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = fmt.Errorf("the script or a binding panicked: %v", r)
		}
	}()
	vm := goja.New()
	for name, value := range globals {
		converted, convErr := jsValue(value, 0)
		if convErr != nil {
			return nil, fmt.Errorf("cannot bind global %q: %w", name, convErr)
		}
		if bindErr := vm.Set(name, converted); bindErr != nil {
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

// jsValue prepares one Go value for the boundary into the script, so every
// message crosses as the plain JavaScript values a decoded JSON message
// would be, whatever Go types carried it:
//
//   - A json.Number - the type a structured message carries - is a number:
//     goja would bind that string-kind type as a string, and the script
//     would see msg.setpoint === 1.5 fail on a message that arrived as
//     JSON. A json.Number that no Go parser reads as a finite number, such
//     as 1e999, crosses as its own text rather than as a broken number.
//   - Every map whose keys are strings or integers becomes a plain object,
//     integer keys in their decimal form; every slice and array becomes a
//     plain array, a nil one an empty array. That holds for typed
//     containers ([]int, map[string]int, nested ones) too, which goja would
//     otherwise bind by reflection, so Array.isArray would be false and a
//     script's write would reach the caller's own map or backing array.
//     The containers are copied, so the caller's message is never written to.
//   - A float64 or float32 that is not finite crosses as the JavaScript
//     number it is: Infinity, -Infinity or NaN. success refuses to return
//     one, because it cannot be encoded as JSON.
//   - A value nested deeper than maxMessageDepth is refused with an error.
//     A message that contains itself, such as a map holding itself, would
//     otherwise be copied until the Go stack overflows, which kills the
//     process and cannot be recovered. A decoded JSON message cannot
//     contain itself and is never nested that deep.
//
// Everything else crosses as it is. depth is how many containers enclose v.
func jsValue(v any, depth int) (any, error) {
	if depth > maxMessageDepth {
		return nil, fmt.Errorf(
			"the message is nested more than %d levels deep, or contains itself", maxMessageDepth)
	}
	switch t := v.(type) {
	case nil, string, bool, int, int64, float64:
		return v, nil
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, nil
		}
		if f, err := t.Float64(); err == nil {
			return f, nil
		}
		return t.String(), nil
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			c, err := jsValue(e, depth+1)
			if err != nil {
				return nil, err
			}
			out[k] = c
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			c, err := jsValue(e, depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = c
		}
		return out, nil
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Map:
		key := mapKey(rv.Type().Key().Kind())
		if key == nil {
			return v, nil
		}
		out := make(map[string]any, rv.Len())
		for it := rv.MapRange(); it.Next(); {
			c, err := jsValue(it.Value().Interface(), depth+1)
			if err != nil {
				return nil, err
			}
			out[key(it.Key())] = c
		}
		return out, nil
	case reflect.Slice, reflect.Array:
		out := make([]any, rv.Len())
		for i := range out {
			c, err := jsValue(rv.Index(i).Interface(), depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = c
		}
		return out, nil
	default:
		return v, nil
	}
}

// maxMessageDepth is how deeply jsValue nests containers before it refuses
// the message as one that contains itself.
const maxMessageDepth = 1000

// mapKey renders a map key of the given kind as an object key, or is nil
// for a kind that has no natural object key, so the map crosses as it is.
func mapKey(kind reflect.Kind) func(reflect.Value) string {
	switch kind {
	case reflect.String:
		return func(k reflect.Value) string { return k.String() }
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return func(k reflect.Value) string { return strconv.FormatInt(k.Int(), 10) }
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return func(k reflect.Value) string { return strconv.FormatUint(k.Uint(), 10) }
	default:
		return nil
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
// The entries are the dispatch boundary of a cleanup block: every
// operation a block can ask for goes through one of them, so a failure
// that surfaces at one means the block did not hand the machine back,
// whatever line inside the operation produced it. Each entry records it
// (cleanupEvidence), whichever of three ways it arrives:
//
//   - The operation returns an error, such as a refused write or a wait
//     that ran out. The entry records it before throwing it to the script.
//   - A callback the script passed, such as the wait's predicate, throws.
//     goja unwinds the entry's Go frame by panic, so nothing returns. The
//     entry recovers the panic, records it and re-raises the same value,
//     so the script sees the same exception (recoverEvidence).
//   - An argument fails its conversion before the operation runs. The
//     entry converts its own arguments, so the failure is an error it
//     records and then throws as a TypeError with goja's wording
//     (argObject and the other arg functions).
//
// read is the one exempt entry: it cannot change the machine, so a
// cleanup may probe, catch the refusal, and carry on. A read inside
// wait_until is not exempt, because that error surfaces at the wait's
// entry, not read's.
func (e *Engine[UnifiedAddress]) namespace(rs *runState, endings bool) map[string]any {
	ns := map[string]any{
		"read": func(call goja.FunctionCall, vm *goja.Runtime) goja.Value {
			// No cleanupEvidence: a read cannot change the machine, so its
			// refusal, a bad argument included, is not the cleanup's failure.
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
			var pollMs *float64
			if v, ok := opts["pollMs"]; ok && v != nil {
				ms := optNumber(vm, v)
				pollMs = &ms
			}
			v, err := e.waitUntil(rs, raw, pred, timeoutMs, pollMs, opts)
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

// recoverEvidence records a panic that unwinds through a dispatch entry as
// the cleanup's evidence, then re-raises the same value, so control flow
// does not change (see namespace). A callback the script passed into an
// operation, such as wait_until's predicate, that throws does not return
// through the entry: goja unwinds the Go frame by panic. An interrupt and a
// Go runtime fault pass through the same way, recorded and then raised
// untouched.
//
// own marks the throws the entry raised itself, whose evidence the entry
// already recorded, so a failure is recorded exactly once. Each entry
// defers recoverEvidence directly, because recover only sees a panic when
// the deferred function calls it itself.
func (rs *runState) recoverEvidence(op string, own *bool) {
	x := recover()
	if x == nil {
		return
	}
	if !*own {
		rs.cleanupEvidence(op, panicAsError(x))
	}
	panic(x) // the same value unwinds on, so control flow does not change
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
// passes whole. The entry converts its own arguments rather than leaving it
// to goja's reflective wrapper, which raises its TypeError before the
// entry's body runs, where the entry cannot record it. The message keeps
// the wording of goja's reflective conversion. A missing or null argument
// converts to a nil address, so ParseAddress owns that refusal.
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
// whichever it fits, and both convert. A missing or null argument is 0.
func argNumber(call goja.FunctionCall, i int) (float64, error) {
	v := call.Argument(i)
	if v == goja.Undefined() || v == goja.Null() {
		return 0, nil
	}
	if n, ok := number(v.Export()); ok {
		return n, nil
	}
	// Like goja's reflective conversion, accept what ToFloat accepts,
	// such as a numeric string or a boolean. A timeout read out of a
	// message often arrives as a string.
	if f := v.ToFloat(); !math.IsNaN(f) {
		return f, nil
	}
	return 0, fmt.Errorf(
		"could not convert function call parameter %d: could not convert %v to float64", i, v)
}

// optNumber converts a numeric option the way argNumber converts a numeric
// argument: a JS number in whichever Go type carried it, and otherwise what
// ToFloat makes of it, so a numeric string is accepted. Anything that is no
// number comes back as NaN, for the caller to refuse with its own message.
func optNumber(vm *goja.Runtime, v any) float64 {
	if n, ok := number(v); ok {
		return n
	}
	return vm.ToValue(v).ToFloat()
}

// argPredicate converts wait_until's second argument into the Go callback
// the wait polls with; see argObject for why the entry owns the conversion.
// A missing or null predicate stays nil, so waitUntil's own error names
// that wrong script; a predicate that is not a function fails the
// conversion like any other argument. The callback reads the result with
// ToBoolean, so a truthy non-boolean satisfies the wait as JavaScript says
// it should, and a throw unwinds the Go frame as a panic, for the entry to
// record (recoverEvidence).
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
			panic(err) // the callback's throw unwinds the entry's frame
		}
		// A truthy non-boolean, such as v.length, 1 or "ok", satisfies the
		// wait. A Go type assertion would read every one of them as false.
		return res.ToBoolean()
	}, nil
}

// address is the first step of every operation that reaches the binding.
// After the run has ended it refuses with errEnded: the ending functions
// abort the script by throwing errEnded, and a throw is catchable, so the
// operations refuse too. Once the run's clock has run out it refuses with
// the context's error. Otherwise it parses the script's address, and
// records a refused parse like any operation's fault, so what a binding
// marks is what Run reports.
func (e *Engine[UnifiedAddress]) address(rs *runState, raw map[string]any) (UnifiedAddress, error) {
	var none UnifiedAddress
	if rs.end != endNone {
		return none, errEnded
	}
	if err := rs.ctx.Err(); err != nil {
		return none, err
	}
	addr, err := e.bind.ParseAddress(raw)
	if err != nil {
		return none, rs.fail("parse address", err)
	}
	return addr, nil
}

// readWithin is readOp bounded by the caller's own deadline rather than the
// run's. It reports whether the read was cut off by that bound while the run
// still had time, which is a wait running out rather than a machine fault --
// and so is NOT recorded as one.
func (e *Engine[UnifiedAddress]) readWithin(
	rs *runState, raw map[string]any, bound time.Time,
) (value any, cutOff bool, err error) { //nolint:nonamedreturns // three results, and the middle one is the question
	addr, err := e.address(rs, raw)
	if err != nil {
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
		return nil, false, rs.fail("read "+e.bind.Describe(addr), err)
	}
	return resp.Value, false, nil
}

// readOp runs one read and records the fault if it failed.
func (e *Engine[UnifiedAddress]) readOp(rs *runState, raw map[string]any) (any, error) {
	addr, err := e.address(rs, raw)
	if err != nil {
		return nil, err
	}
	resp, err := e.bind.Read(rs.ctx, addr)
	if err != nil {
		return nil, rs.fail("read "+e.bind.Describe(addr), err)
	}
	return resp.Value, nil
}

// writeOp sends one value, attempted once. A failure is recorded like any
// operation's, for the script's outcome; inside a cleanup block the
// dispatch boundary records what a refused write means (cleanupEvidence):
// a refused write leaves the machine's state unknown, which a cleanup
// block must not be able to catch its way around.
func (e *Engine[UnifiedAddress]) writeOp(rs *runState, raw map[string]any, value any) error {
	addr, err := e.address(rs, raw)
	if err != nil {
		return err
	}
	if err := e.bind.Write(rs.ctx, addr, value); err != nil {
		return rs.fail("write "+e.bind.Describe(addr), err)
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
	addr, err := e.address(rs, raw)
	if err != nil {
		return nil, err
	}
	resp, err := caller.Call(rs.ctx, addr, inputs)
	if err != nil {
		return nil, rs.fail("call "+e.bind.Describe(addr), err)
	}
	return resp.Outputs, nil
}
