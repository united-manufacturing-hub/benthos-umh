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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"
)

// ending is how a script said its run ended.
type ending uint8

const (
	endNone        ending = iota // the script has not ended yet
	endSuccess                   // success(obj)
	endNotReady                  // error_not_ready()
	endTransient                 // error_transient(detail)
	endPersistent                // error_persistent(detail)
	endWaitTimeout               // a wait_until ran out; see waitPersistent
)

// runState is what one run decides, from the script's calls to Run's result.
// The ending functions abort the script by throwing, and once one has run
// every operation refuses, so a script that catches the throw cannot touch
// the machine again; the first ending wins.
type runState struct {
	ctx            context.Context // the clock the block runs under
	deadline       time.Time       // zero when the block has no deadline
	end            ending
	data           any    // the object success returned
	detail         any    // the detail an ending call passed
	opErr          error  // the binding operation the run's clock cut off, if one was in flight
	opWhere        string // which one, for the message of a run its deadline stopped
	mutErr         error  // the first operation of a cleanup block that failed: the machine was not handed back (cleanupEvidence)
	mutWhere       string // which one, for the cleanup's error message
	waitPersistent bool   // a wait that ran out was told onTimeout persistent
	cleanup        bool   // this run is a cleanup block, which has no ending to record
}

// finish records the script's ending; the first one wins.
func (rs *runState) finish(end ending, detail any) bool {
	if rs.end != endNone {
		return false
	}
	rs.end = end
	rs.detail = detail
	return true
}

// fail returns the *fault a failed binding operation throws into the
// script. Result.Err wraps the binding's error, so a caller can inspect it
// with errors.Is and errors.As; which fault decides the outcome is
// classify's question.
//
// The operation is also recorded, to name it in the message of a run its
// deadline stopped, but only when it failed after the run's clock ran out:
// that is the operation the deadline cut off. A fault from before then is
// either in the run's error, if the script did not catch it, or caught, and
// a caught fault must not explain a stop it had no part in, nor make
// errors.Is find the binding's error in a deadline's Result.Err.
func (rs *runState) fail(where string, err error) error {
	if rs.ctx.Err() != nil {
		rs.opErr = err
		rs.opWhere = where
	}
	return &fault{where: where, err: err}
}

// A fault is a binding operation that failed, as the operation throws it
// into the script. It reads as the binding's own error, so a script that
// catches it sees the binding's text, and it travels with the exception:
// whatever stops the run, classify finds the fault in it with errors.As,
// however many faults the script caught before and whatever failed while
// the stack unwound.
type fault struct {
	where string // the operation, such as "write ns=2;s=Apply"
	err   error  // the binding's error
}

func (f *fault) Error() string { return f.err.Error() }
func (f *fault) Unwrap() error { return f.err }

// failMutating records the evidence that a cleanup block did not hand the
// machine back: the block asked for an operation and the operation came
// back with an error, so the machine's state is unknown whether the block's
// JavaScript caught the throw or not. The first such failure is kept. Its
// only caller is cleanupEvidence, so every failure route out of an entry
// is covered at the dispatch boundary rather than by a guard at each
// failure site.
func (rs *runState) failMutating(where string, err error) {
	if rs.mutErr == nil {
		rs.mutErr = err
		rs.mutWhere = where
	}
}

// cleanupEvidence records a failure that surfaced at one of a cleanup
// block's dispatch entries (see namespace) as evidence that the block did
// not hand the machine back. In the script, where rs.cleanup is false, it
// does nothing: the script's outcome is classified from its own endings and
// faults.
//
// errEnded is not evidence: it is the engine declining to touch the machine
// after the run has ended, not the machine refusing. The read entry does not
// call cleanupEvidence, because a read cannot change the machine, so a
// cleanup may probe, catch the refusal and carry on. A read inside a
// wait_until is evidence, because its error surfaces at the wait's entry:
// the wait was abandoned, so the work the block meant to do after it never
// ran.
//
// The failure is named by the fault the operation recorded for this error,
// such as "write ns=2;s=Reset", or else by op, the entry the block called.
func (rs *runState) cleanupEvidence(op string, err error) {
	if err == nil || !rs.cleanup || errors.Is(err, errEnded) {
		return
	}
	where := op
	var f *fault
	if errors.As(err, &f) {
		where = f.where
	}
	rs.failMutating(where, err)
}

// errEnded aborts the script once it has ended; the engine reads the ending
// from the run state, not from this text.
var errEnded = errors.New("the script ended")

// An Outcome is how one run ended. Transient means the machine was left ready to be
// asked again; Persistent means it was not, or the fault will not resolve by repeating.
type Outcome uint8

const (
	// Delivered is not an error: the machine received the write request and acted on
	// it, and Result.Data holds what the script returned.
	Delivered Outcome = iota

	// Transient means the fault could resolve on its own and the cleanup left the
	// machine ready, so the message is safe to run again unchanged.
	Transient

	// Persistent means the fault will not resolve by repeating, or the cleanup did
	// not finish, so the machine's state is unknown.
	Persistent
)

// A Result is everything one run produces. There is no accompanying error: a fault is
// an outcome of this package's job, not a failure to do it, and returning both would
// allow states that cannot occur.
type Result struct {
	// Outcome is how the run ended, and the only field a caller has to branch on.
	Outcome Outcome

	// Data is what the script returned to success, and is nil otherwise.
	Data any

	// Err says what went wrong, and is nil when Outcome is Delivered. It wraps the
	// binding's own error, so a caller can inspect it with errors.Is and errors.As.
	Err error

	// CleanupErr reports that OnSuccess or OnError did not finish, and names which. It
	// never changes a delivered outcome; after a failure it makes the outcome Persistent.
	CleanupErr error
}

// A Config is one handshake as the user wrote it. Validate it before constructing an
// Engine; New does so itself and refuses an invalid one.
type Config struct {
	// Script is the handshake, in JavaScript. It runs once per message and must end by
	// returning success, error_not_ready, error_transient or error_persistent.
	Script string

	// OnSuccess runs after success, OnError after every other ending, including the
	// deadline. Either block may be empty; both must be declared.
	OnSuccess string
	OnError   string

	// The three deadlines. MaxScriptRuntime must comfortably exceed the waits the script
	// performs: it stops a runaway, not paces a machine.
	MaxScriptRuntime    time.Duration
	MaxOnSuccessRuntime time.Duration
	MaxOnErrorRuntime   time.Duration

	// PollInterval is how often wait_until re-reads when a call does not say. A script
	// overrides it per call with the pollMs option.
	PollInterval time.Duration
}

// MaxInterval is the longest a handshake may wait for anything: its own
// deadline, a cleanup budget, or a single wait_until. A handshake is one
// machine acknowledging one instruction, so an hour is not a long handshake,
// it is a mistake -- and the engine refuses it rather than letting a message
// sit on a machine while a bridge looks healthy.
const MaxInterval = time.Hour

// maxIntervalMillis is MaxInterval in milliseconds, for checks that have to
// compare before converting to a Duration.
const maxIntervalMillis = float64(MaxInterval / time.Millisecond)

// Validate reports whether the configuration can run. It compiles all three blocks, so
// a script that does not parse is caught at deploy time rather than against a machine.
func (c Config) Validate() error {
	_, err := c.compileBlocks()
	return err
}

// compileBlocks validates the configuration and returns its compiled blocks, so
// New compiles each block once rather than once to validate and again to run.
func (c Config) compileBlocks() (map[string]*goja.Program, error) {
	if c.MaxScriptRuntime <= 0 {
		return nil, fmt.Errorf("MaxScriptRuntime must be positive, is %v", c.MaxScriptRuntime)
	}
	// A handshake is one machine's acknowledgement of one instruction,
	// measured in seconds, so a budget over MaxInterval is a unit mistake,
	// such as a value carried from somewhere that meant something else.
	// Accepting it would let a message sit on a machine for an hour before
	// anyone learns it failed. Config time is where refusing it is cheap:
	// once the pipeline runs, the symptom is a bridge that looks healthy and
	// delivers nothing.
	for _, budget := range []struct {
		name  string
		value time.Duration
	}{
		{"MaxScriptRuntime", c.MaxScriptRuntime},
		{"MaxOnSuccessRuntime", c.MaxOnSuccessRuntime},
		{"MaxOnErrorRuntime", c.MaxOnErrorRuntime},
	} {
		if budget.value > MaxInterval {
			return nil, fmt.Errorf(
				"%s is %v, longer than the %v a handshake is allowed: a budget this long is a unit mistake far more often than an intention, and a message would sit on the machine that long before anyone learned it failed",
				budget.name, budget.value, MaxInterval)
		}
	}
	// A cleanup budget of zero or less means unbounded, so a non-empty
	// block without a budget would hang Run and Close on a machine that
	// never answers, and an omitted YAML field is enough to get there.
	// Either block may be empty; a non-empty one must be budgeted, and the
	// refusal names both.
	for _, block := range []struct {
		name, budgetName, source string
		budget                   time.Duration
	}{
		{"onSuccess", "MaxOnSuccessRuntime", c.OnSuccess, c.MaxOnSuccessRuntime},
		{"onError", "MaxOnErrorRuntime", c.OnError, c.MaxOnErrorRuntime},
	} {
		if strings.TrimSpace(block.source) == "" {
			continue
		}
		if block.budget <= 0 {
			return nil, fmt.Errorf("%s is not empty, so %s must be positive, is %v",
				block.name, block.budgetName, block.budget)
		}
	}
	// Each block is parsed once: the AST is compiled to the program the
	// engine runs, and read for the literal waits below.
	//
	// A block whose literal waits add up to more than its own budget is
	// killed on a slow day, mid-commit. This check is an early, friendly
	// error; the safety mechanism is wait_until clamping its timeout to the
	// deadline. The waits are read from the parsed program, so a wait in a
	// comment or a string is not a wait, and none hides behind a template
	// or regex literal.
	blocks := make(map[string]*goja.Program, 3)
	for _, block := range []struct {
		name, budgetName, source string
		budget                   time.Duration
	}{
		{"script", "MaxScriptRuntime", c.Script, c.MaxScriptRuntime},
		{"onSuccess", "MaxOnSuccessRuntime", c.OnSuccess, c.MaxOnSuccessRuntime},
		{"onError", "MaxOnErrorRuntime", c.OnError, c.MaxOnErrorRuntime},
	} {
		program, tree, err := compile(block.name, block.source)
		if err != nil {
			return nil, err
		}
		blocks[block.name] = program
		if err := checkWaitOptions(tree); err != nil {
			return nil, fmt.Errorf("%s: a wait_until %w", block.name, err)
		}
		waits := literalWaitTotal(tree)
		if waits > block.budget {
			return nil, fmt.Errorf(
				"%s's literal waits total %v, which exceeds %s %v",
				block.name, waits, block.budgetName, block.budget)
		}
	}
	return blocks, nil
}

// An Engine runs one configured handshake against one machine, once per message.
type Engine[UnifiedAddress any] struct {
	bind Binding[UnifiedAddress]
	cfg  Config
	_    [0]func() // not comparable

	blocks map[string]*goja.Program // compiled script, onSuccess, onError
	mu     sync.Mutex               // one transaction at a time; Close waits for it
}

// New compiles the configuration and returns an Engine bound to one binding. It returns
// an error only for a configuration that cannot run; everything that can go wrong later
// is reported per run, in a Result.
func New[UnifiedAddress any](
	cfg Config, p Binding[UnifiedAddress],
) (*Engine[UnifiedAddress], error) {
	blocks, err := cfg.compileBlocks()
	if err != nil {
		return nil, err
	}
	return &Engine[UnifiedAddress]{bind: p, cfg: cfg, blocks: blocks}, nil
}

// Run executes the handshake once for one message, where msg is the message as the
// script sees it. Concurrent calls on the same Engine run one after another, because
// a machine takes one transaction at a time. Canceling ctx ends the run as a deadline
// does; OnError still runs.
func (e *Engine[UnifiedAddress]) Run(ctx context.Context, msg map[string]any) Result {
	e.mu.Lock()
	defer e.mu.Unlock()

	// The script's own deadline, which a canceled caller ends the same way.
	// It stops a runaway, not paces a machine.
	scriptCtx, cancel := context.WithTimeout(ctx, e.cfg.MaxScriptRuntime)
	defer cancel()
	deadline, _ := scriptCtx.Deadline()
	rs := &runState{ctx: scriptCtx, deadline: deadline}
	globals := map[string]any{
		e.bind.Namespace(): e.namespace(rs, true), // the script sees the machine
		"msg":              msg,
	}
	_, runErr := runProgram(scriptCtx, e.blocks["script"], globals)
	result := classify(scriptCtx, rs, runErr)

	// Exactly one of the two blocks runs after every run, including one the
	// deadline stopped. The block runs on its own clock, not the
	// caller's, so a canceled parent does not stop the machine being handed
	// back; and the cleanup blocks see nothing but the machine.
	block, budget := "onError", e.cfg.MaxOnErrorRuntime
	if result.Outcome == Delivered {
		block, budget = "onSuccess", e.cfg.MaxOnSuccessRuntime
	}
	if err := e.runCleanup(block, budget); err != nil {
		// A delivery stands, carrying the cleanup failure beside it.
		// Anything else is persistent, whatever the fault was.
		result.CleanupErr = err
		if result.Outcome != Delivered {
			result.Outcome = Persistent
		}
	}
	return result
}

// runCleanup runs one cleanup block once, with a budget of zero or less
// meaning the block runs unbounded. The block did not finish if its
// JavaScript aborted, or if an operation it asked for came back with an
// error, caught or not, because in both the machine was not returned to a
// state it can be asked from. The operations' failures are recorded at the
// dispatch boundary (cleanupEvidence); runCleanup only reads them.
func (e *Engine[UnifiedAddress]) runCleanup(block string, budget time.Duration) error {
	ctx := context.Background()
	if budget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()
	}
	cleanupDeadline, _ := ctx.Deadline()
	rs := &runState{ctx: ctx, deadline: cleanupDeadline, cleanup: true}
	globals := map[string]any{e.bind.Namespace(): e.namespace(rs, false)}
	_, err := runProgram(ctx, e.blocks[block], globals)
	if err != nil {
		return fmt.Errorf("%s did not finish: %w", block, err)
	}
	if rs.mutErr != nil {
		// The block's JavaScript ended cleanly, but an operation it asked
		// for failed, and the block caught the error. The machine was not
		// handed back, however the block carried on.
		return fmt.Errorf("%s did not finish: %s failed: %w", block, rs.mutWhere, rs.mutErr)
	}
	return nil
}

// Close waits for a Run already in flight to finish. Each run builds and
// discards its own runtime, so there is nothing else to release.
func (e *Engine[UnifiedAddress]) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return nil
}
