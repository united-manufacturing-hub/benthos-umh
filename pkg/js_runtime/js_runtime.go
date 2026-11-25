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
	"errors"
	"time"
)

// errNotImplemented is a placeholder while this package is built test-first.
var errNotImplemented = errors.New("js_runtime: not implemented")

// A Program is a compiled script. Compile applies the function wrap that
// every caller in this repo already uses, (function(){ 'use strict'; ... })(),
// so a script may use a top-level return. goja rejects a top-level return in
// unwrapped source.
type Program struct{ /* unexported */ }

// Compile compiles source under the wrap. name appears in error messages.
func Compile(_, _ string) (*Program, error) {
	return nil, errNotImplemented
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

// A Runner executes compiled programs on pooled runtimes.
type Runner struct{ /* unexported */ }

// NewRunner returns a Runner. It never fails.
func NewRunner(_ Options) *Runner {
	return &Runner{}
}

// Run executes p with globals bound into the runtime, and returns the script's
// completion value exported to Go. The deadline comes from ctx: Run
// interrupts the runtime when ctx is done and returns ctx.Err() wrapped. A
// runtime that does not stop after the interrupt is discarded rather than
// pooled.
func (*Runner) Run(_ context.Context, _ *Program, _ map[string]any) (any, error) {
	return nil, errNotImplemented
}

// Close releases pooled runtimes.
func (*Runner) Close() error {
	return errNotImplemented
}
