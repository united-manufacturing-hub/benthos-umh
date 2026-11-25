package handshake

import (
	"context"
	"errors"
	"fmt"

	"github.com/dop251/goja"
)

// scriptRuntime is the seam to pkg/js_runtime (PR0, ENG-8128): Compile once,
// run pooled, interrupt on ctx, close. The real package satisfies this shape
// through an adapter when the branches are reconciled; until then newRuntime
// builds the goja-backed stand-in below, and nothing else changes.
type scriptRuntime interface {
	compile(name, source string) (any, error)
	run(ctx context.Context, program any, globals map[string]any) (any, error)
	close() error
}

// newRuntime builds the scriptRuntime an Engine runs on. It is a variable so
// the reconciliation with pkg/js_runtime swaps the implementation in one
// place, and so tests substitute a fake where a real runtime would be slow.
var newRuntime = func() scriptRuntime { return gojaRuntime{} }

// gojaRuntime is the stand-in for pkg/js_runtime. It builds a fresh goja
// runtime per run, which is the strict reset LIB-7 requires; pooling with a
// global reset is PR0's to bring, not something this package re-invents.
type gojaRuntime struct{}

// Compile applies the function wrap every caller in this repo already uses,
// (function(){ 'use strict'; ... })(), so a script may use a top-level return.
func (gojaRuntime) compile(name, source string) (any, error) {
	wrapped := "(function(){\n'use strict';\n" + source + "\n})()"
	program, err := goja.Compile(name, wrapped, false)
	if err != nil {
		return nil, fmt.Errorf("%s does not compile: %w", name, err)
	}
	return program, nil
}

// Run binds globals into a fresh runtime and executes program. The deadline
// comes from ctx: the watcher goroutine interrupts the runtime when ctx is
// done, and the returned error wraps ctx.Err(). The runtime is discarded
// rather than pooled, so an interrupt that arrives late cannot hurt the next
// run.
func (gojaRuntime) run(ctx context.Context, program any, globals map[string]any) (any, error) {
	prog, ok := program.(*goja.Program)
	if !ok {
		return nil, fmt.Errorf("program was not compiled by this runtime")
	}
	vm := goja.New()
	for name, value := range globals {
		if err := vm.Set(name, value); err != nil {
			return nil, fmt.Errorf("cannot bind global %q: %w", name, err)
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
	value, err := vm.RunProgram(prog)
	close(stop)
	<-watchDone
	if err != nil {
		return nil, wrapRunError(err)
	}
	return value.Export(), nil
}

// Close releases pooled runtimes. The stand-in pools none, so there is
// nothing to release until the reconciliation replaces it.
func (gojaRuntime) close() error { return nil }

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
// function is a plain Go func the runtime binds by reflection, so the seam
// carries no runtime type. Endings are offered to the script only: a cleanup
// block returns the machine to idle and has nothing to end.
func (e *Engine[UnifiedAddress]) namespace(rs *runState, endings bool) map[string]any {
	ns := map[string]any{
		"read": func(raw map[string]any) (any, error) {
			return e.readOp(rs, raw)
		},
		"write": func(raw map[string]any, value any) error {
			return e.writeOp(rs, raw, value)
		},
		"wait_until": func(raw map[string]any, pred func(any) bool, timeoutMs float64, opts map[string]any) (any, error) {
			return e.waitUntil(rs, raw, pred, timeoutMs, opts)
		},
	}
	if caller, ok := e.bind.(Caller[UnifiedAddress]); ok {
		ns["call_method"] = func(raw map[string]any, inputs []any) (any, error) {
			return e.callOp(rs, caller, raw, inputs)
		}
	}
	if endings {
		ns["success"] = func(data any) error {
			if rs.finish(endSuccess, nil) {
				rs.data = data
			}
			return errEnded
		}
		ns["error_not_ready"] = func() error {
			rs.finish(endNotReady, nil)
			return errEnded
		}
		ns["error_transient"] = func(detail any) error {
			rs.finish(endTransient, detail)
			return errEnded
		}
		ns["error_persistent"] = func(detail any) error {
			rs.finish(endPersistent, detail)
			return errEnded
		}
	}
	return ns
}

// readOp runs one read and records the fault if it failed. A parse failure is
// recorded like a read failure: what a binding marks is what Run reports.
// After the run has ended the machine is not touched again: the ending
// functions abort the script by throwing errEnded, and a throw is catchable,
// so the operations themselves refuse too (UAT-13).
func (e *Engine[UnifiedAddress]) readOp(rs *runState, raw map[string]any) (any, error) {
	if rs.end != endNone {
		return nil, errEnded
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

// writeOp sends one value, attempted once. After the run has ended the
// write is refused without reaching the machine (UAT-13): the first ending
// decided the transaction, and nothing may extend it.
func (e *Engine[UnifiedAddress]) writeOp(rs *runState, raw map[string]any, value any) error {
	if rs.end != endNone {
		return errEnded
	}
	addr, err := e.bind.ParseAddress(raw)
	if err != nil {
		rs.fail("parse address", err)
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
func (e *Engine[UnifiedAddress]) callOp(
	rs *runState, caller Caller[UnifiedAddress], raw map[string]any, inputs []any,
) (any, error) {
	if rs.end != endNone {
		return nil, errEnded
	}
	addr, err := e.bind.ParseAddress(raw)
	if err != nil {
		rs.fail("parse address", err)
		return nil, err
	}
	resp, err := caller.Call(rs.ctx, addr, inputs)
	if err != nil {
		rs.fail("call "+e.bind.Describe(addr), err)
		return nil, err
	}
	return resp.Outputs, nil
}
