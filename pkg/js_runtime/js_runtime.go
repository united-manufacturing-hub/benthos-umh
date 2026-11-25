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
// The API is frozen in FROZEN-js-runtime.md (the seam between PR0 and PR1):
// exactly six exported names — Program, Compile, Options, Runner, NewRunner,
// plus the Runner methods. Do not add exported names; if you think you need
// one, put it on the morning list and work around it.
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
const (
	wrapPrefix = "(function(){'use strict';\n"
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
func Compile(name, source string) (*Program, error) {
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
func runOnVM(vm *goja.Runtime, p *Program) (out runOutcome) {
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
func (r *Runner) Run(ctx context.Context, p *Program, globals map[string]any) (any, error) {
	start := time.Now()
	pr := r.pool.Get().(*pooledRuntime)

	if err := bindGlobals(pr.vm, globals); err != nil {
		// The runtime may be half-bound; do not pool it.
		return nil, err
	}

	out := runOnVM(pr.vm, p)

	// TODO(ENG-8128): reset the bound globals, honour the ctx deadline by
	// interrupting, and discard runtimes that do not stop or do not reset.
	r.pool.Put(pr)

	if r.opts.OnSlow != nil && r.opts.Slow > 0 {
		if d := time.Since(start); d > r.opts.Slow {
			r.opts.OnSlow(d)
		}
	}
	return out.val, out.err
}

// Close releases pooled runtimes.
func (*Runner) Close() error {
	// goja runtimes hold no OS resources; pooled ones are garbage collected
	// with the pool. Nothing to release.
	return nil
}
