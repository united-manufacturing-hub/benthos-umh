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

// Package js_runtime is the one JavaScript runtime mechanism for benthos-umh.
//
// Its API is deliberately six exported names — Program, Compile, Options,
// Runner, NewRunner, and the Runner methods — and is meant to stay that way.
// It is the seam the handshake engine (ENG-8129) is built against, so every
// name added here is one that package can come to depend on, and widening a
// seam is easy while narrowing one is not. If you think you need a seventh,
// work around it and raise it on ENG-8128 instead.
package js_runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dop251/goja"
)

// The wrap every caller in this repo already applies before compiling user
// scripts (nodered_js_plugin.go and tag_processor_plugin.go both did this
// inline; this package makes it the default). The 'use strict' directive is
// part of the wrap, so scripts run strict, and a top-level return is legal.
// The prefix is three lines, and deliberately so. The callers this package
// replaced wrapped their scripts as
//
//	\n\t\t(function(){\n\t\t\t'use strict';\n\t\t<source>\n\t\t})()\n
//
// which put a user's first line on line 4. A one-line prefix puts it on line
// 2, and every line number in every error a customer sees moves by two:
// measured against the base commit, a throw reported at nodered-fn.js:4
// started reporting at :2, and its caller frame moved from :6 to :4. The
// line number is how someone finds the fault in a two-hundred-line function,
// and it reaches their logs and their drop reasons, so keeping the count
// identical is worth two newlines.
//
// Columns are not preserved. The old wrap indented the source with two tabs
// and goja counted them, so a column reads three lower here. That was an
// artifact of the old wrapper rather than anything about the script, and
// reproducing it would mean indenting every user's code to keep a number
// that pointed at whitespace.
const (
	wrapPrefix = "\n\n(function(){'use strict';\n"
	wrapSuffix = "\n})()"
)

// A Program is a compiled script. Compile applies the function wrap that
// every caller in this repo already uses, (function(){ 'use strict'; ... })(),
// so a script may use a top-level return. goja rejects a top-level return in
// unwrapped source.
type Program struct {
	prog *goja.Program
}

// Compile compiles source under the wrap. name appears in error messages.
func Compile(name string, source string) (*Program, error) {
	prog, err := goja.Compile(name, wrapPrefix+source+wrapSuffix, false)
	if err != nil {
		return nil, err
	}
	return &Program{prog: prog}, nil
}

// Options configure a Runner. The zero value is a plain pooled runtime with no
// hardening and no reset, which is what nodered_js has today.
type Options struct {
	// Harden installs the sandbox blocklist. See ENG-8128: the blocklist as it
	// exists today does not hold, so a caller that sets this gets what
	// stream_processor has, not a guarantee.
	Harden bool

	// ResetGlobals clears the global object between runs, rather than only
	// the message.
	ResetGlobals bool

	// OnSlow, when set, is called with the duration of a run that exceeded Slow.
	OnSlow func(time.Duration)
	Slow   time.Duration
}

// pooledRuntime is one pooled goja runtime. When ResetGlobals is set, pristine
// holds the global object's own property names recorded at creation, so the
// reset can delete everything else.
type pooledRuntime struct {
	vm       *goja.Runtime
	pristine map[string]struct{}
}

// A Runner executes compiled programs on pooled runtimes.
type Runner struct {
	opts Options
	pool sync.Pool
}

// NewRunner returns a Runner. It never fails.
func NewRunner(opts Options) *Runner {
	r := &Runner{opts: opts}
	r.pool.New = func() any { return r.newRuntime() }
	return r
}

func (r *Runner) newRuntime() *pooledRuntime {
	vm := goja.New()
	if r.opts.Harden {
		harden(vm)
	}
	pr := &pooledRuntime{vm: vm}
	if r.opts.ResetGlobals {
		pristine := make(map[string]struct{})
		for _, k := range vm.GlobalObject().GetOwnPropertyNames() {
			pristine[k] = struct{}{}
		}
		pr.pristine = pristine
	}
	return pr
}

// runOutcome is the result of executing one program on one runtime.
type runOutcome struct {
	val any
	err error
}

// runOnVM executes p on vm, converting a Go panic inside the engine into an
// error (goja does not recover panics itself; letting one escape would take
// down the caller's goroutine).
func runOnVM(vm *goja.Runtime, p *Program) (out runOutcome) { //nolint:nonamedreturns // a deferred recover assigns to it; naming is the mechanism
	defer func() {
		if rec := recover(); rec != nil {
			out.val, out.err = nil, fmt.Errorf("JavaScript execution panic: %v. Please check the expression for runtime errors", rec)
		}
	}()
	v, err := vm.RunProgram(p.prog)
	if err != nil {
		return runOutcome{err: err}
	}
	return runOutcome{val: v.Export()}
}

// interruptGrace is how long Run waits for a script to stop after the
// context deadline hit and the runtime was interrupted, before giving up on
// the runtime. Matches the grace the stream processor's watchdog had.
const interruptGrace = time.Second

// bindGlobals sets every entry of globals on the runtime's global object.
func bindGlobals(vm *goja.Runtime, globals map[string]any) error {
	for name, value := range globals {
		if err := vm.Set(name, value); err != nil {
			return fmt.Errorf("failed to bind global %q: %w", name, err)
		}
	}
	return nil
}

// Run executes p with globals bound into the runtime, and returns the script's
// completion value exported to Go. The deadline comes from ctx: Run
// interrupts the runtime when ctx is done and returns ctx.Err() wrapped. A
// runtime that does not stop after the interrupt is discarded rather than
// pooled.
//
// A context with no deadline runs inline on the calling goroutine, with no
// interruption at all — the no-time-limit path nodered_js and tag_processor
// use. Interruption requires a deadline: a cancelable-but-undated context is
// ignored, like the callers' own contexts are today.
func (r *Runner) Run(ctx context.Context, p *Program, globals map[string]any) (any, error) {
	start := time.Now()
	defer func() {
		if r.opts.OnSlow == nil || r.opts.Slow <= 0 {
			return
		}
		if d := time.Since(start); d > r.opts.Slow {
			r.opts.OnSlow(d)
		}
	}()

	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		return r.runInline(ctx, p, globals)
	}
	return r.runWithDeadline(ctx, p, globals)
}

// runInline is the zero-deadline path: no goroutine, no watcher, no
// interruption. (ctx is deliberately unused: without a deadline it cannot
// stop a run, and today's no-limit callers never stopped one.)
func (r *Runner) runInline(_ context.Context, p *Program, globals map[string]any) (any, error) {
	pr := r.pool.Get().(*pooledRuntime)

	if err := bindGlobals(pr.vm, globals); err != nil {
		// The runtime may be half-bound; do not pool it.
		return nil, err
	}

	out := runOnVM(pr.vm, p)
	r.release(pr, globals, true)
	return out.val, out.err
}

// runWithDeadline is the watchdog path: the program runs on its own
// goroutine; when ctx is done the runtime is interrupted and given the
// grace period to stop. A runtime that does not stop is discarded.
func (r *Runner) runWithDeadline(ctx context.Context, p *Program, globals map[string]any) (any, error) {
	if err := ctx.Err(); err != nil {
		// Already done: do not even start. Nothing was bound; the contract
		// says the caller gets ctx.Err() wrapped.
		return nil, wrapCtxErr(err)
	}

	pr := r.pool.Get().(*pooledRuntime)

	if err := bindGlobals(pr.vm, globals); err != nil {
		// The runtime may be half-bound; do not pool it.
		return nil, err
	}

	done := make(chan runOutcome, 1)
	go func() {
		done <- runOnVM(pr.vm, p)
	}()

	var out runOutcome
	finished := false
	select {
	case out = <-done:
		finished = true
	case <-ctx.Done():
		// Interrupt the interpreter; it checks the flag at loop boundaries
		// and on function calls.
		pr.vm.Interrupt("js_runtime: context done")
		select {
		case out = <-done:
			finished = true
		case <-time.After(interruptGrace):
			// The script did not stop after the interrupt. Drop the runtime
			// on the floor: the leaked goroutine still references it, so it
			// must never be pooled.
		}
	}

	// ctx.Err() != nil means the run was aborted by the caller's context
	// (even if the script happened to finish at the same moment): the
	// contract says Run returns ctx.Err() wrapped.
	if ctxErr := ctx.Err(); ctxErr != nil {
		r.release(pr, globals, finished)
		return nil, wrapCtxErr(ctxErr)
	}

	r.release(pr, globals, finished)
	return out.val, out.err
}

// release returns a finished runtime to the pool, or discards it. A runtime
// is discarded when the script did not stop after the interrupt (finished ==
// false, the goroutine may still be running on it) or when the reset fails —
// a runtime whose state could not be restored must not be reused.
func (r *Runner) release(pr *pooledRuntime, globals map[string]any, finished bool) {
	if !finished {
		return
	}
	if !r.reset(pr, globals) {
		return
	}
	pr.vm.ClearInterrupt()
	r.pool.Put(pr)
}

// reset restores the runtime's state after a run: the globals the run bound
// (the "message") are set back to undefined, and with ResetGlobals every
// global object property that was not there at creation is deleted. It
// reports whether the runtime is clean again.
func (r *Runner) reset(pr *pooledRuntime, globals map[string]any) bool {
	vm := pr.vm
	for name := range globals {
		if err := vm.Set(name, goja.Undefined()); err != nil {
			return false
		}
	}
	if r.opts.ResetGlobals {
		global := vm.GlobalObject()
		for _, name := range global.GetOwnPropertyNames() {
			if _, pristine := pr.pristine[name]; pristine {
				continue
			}
			if err := global.Delete(name); err != nil {
				return false
			}
		}
	}
	return true
}

func wrapCtxErr(err error) error {
	return fmt.Errorf("js_runtime: run aborted by the context: %w", err)
}

// Close releases pooled runtimes.
func (*Runner) Close() error {
	// goja runtimes hold no OS resources; pooled ones are garbage collected
	// with the pool. Nothing to release.
	return nil
}
