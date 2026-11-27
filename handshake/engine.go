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
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

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
// The ending functions abort the script once one has run, so a script cannot
// carry on past the end of its transaction; the first ending wins.
type runState struct {
	ctx            context.Context // the clock the block runs under
	deadline       time.Time       // zero when the block has no deadline
	end            ending
	data           any             // the object success returned
	detail         any             // the detail an ending call passed
	opErr          error           // the most recent binding operation that failed
	faults         []recordedFault // every fault this run recorded, for classify to match the run's error against
	opWhere        string          // which one, for the error message and for cleanupEvidence's pairing
	mutErr         error           // an operation a cleanup block asked for that came back with an error: the evidence the machine was not handed back, recorded at the dispatch boundary (cleanupEvidence)
	mutWhere       string          // which one, for the cleanup's error message
	waitPersistent bool            // a wait that ran out was told onTimeout persistent
	cleanup        bool            // this run is a cleanup block: no ending to record, so a wait that runs out is cleanup-failure evidence instead
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

// fail records a binding operation that failed; Result.Err wraps it, so a
// caller can inspect it with errors.Is and errors.As.
//
// The LATEST fault wins. A fault the script caught and carried on from did
// not stop the run, so it cannot be the one that explains how the run
// ended (the 2026-11-26 ruling, UAT-8b). Recording the first instead meant
// a caught "machine unreachable" masked the persistent refusal that
// actually ended the script, and the message was retried forever against a
// machine that would never accept it.
//
// Whether a recorded fault reaches the outcome at all is classify's
// question, not this one: an explicit ending outranks it, because reaching
// an ending is the script saying it survived.
func (rs *runState) fail(where string, err error) {
	rs.opErr = err
	rs.opWhere = where
	// Every fault, not only the latest, because classify has to find the
	// one the run's error actually is. Keeping only the latest meant a
	// fault recorded while the stack unwound -- a failing operation inside
	// a finally, or a rethrow -- displaced the fault that stopped the run,
	// and the outcome then depended on whether an unrelated operation in an
	// unwind path also failed.
	//
	// Bounded, because a script can fail as often as it likes and this is
	// only ever searched for a match.
	// The newest, not the oldest. The fault the run's error actually IS is
	// the last one recorded, so a cap that kept the first 32 dropped the
	// very fault this slice exists to find: with 31 caught faults the
	// stopper was recorded and the run was Transient naming the node, with
	// 32 it was dropped and the same machine fault became Persistent with a
	// raw goja dump for an error. The class a flow branches on must not
	// depend on how many times the script recovered first.
	if len(rs.faults) == maxRecordedFaults {
		copy(rs.faults, rs.faults[1:])
		rs.faults = rs.faults[:maxRecordedFaults-1]
	}
	rs.faults = append(rs.faults, recordedFault{where: where, err: err})
}

// maxRecordedFaults bounds what one run keeps: the most recent this many.
// A run that fails more often than this has told us what we need, and the
// cap exists so a looping script cannot grow the slice without limit.
const maxRecordedFaults = 32

// recordedFault is one failed operation, kept so classify can match the
// run's own error against it.
type recordedFault struct {
	where string
	err   error
}

// failMutating records the evidence that a cleanup block did not hand the
// machine back (LIB-4): the block asked for an operation and the operation
// came back with an error, so the machine's state is unknown whether the
// block's JavaScript caught the throw or not. For the script it is the same
// as fail. failMutating has exactly one caller — cleanupEvidence, at the
// dispatch boundary where a cleanup block's operations are dispatched —
// because the first five failure routes were each found open one at a
// time, each patched where it was found, and the sixth (wait_until without
// its predicate, which returns before the machine is touched) proved that
// a guard at a failure site cannot close the series: a route nobody has
// found yet inherits the rule at the boundary instead of growing its own
// guard.
func (rs *runState) failMutating(where string, err error) {
	rs.fail(where, err)
	if rs.mutErr == nil {
		rs.mutErr = err
		rs.mutWhere = where
	}
}

// cleanupEvidence is the dispatch boundary's half of LIB-4. A cleanup
// block's job is to return the machine to a state it can be asked from;
// when a failure surfaces at one of the block's dispatch entries, that job
// did not finish — regardless of which line inside the operation produced
// the failure, or which of the three mechanisms carried it: an operation
// that returns an error, a callback that panics out of the Go frame
// (recoverEvidence), or an argument that fails the entry's own conversion
// before the operation runs — so this is where the evidence is recorded:
// where the namespace dispatches the block's operations, not at each
// failure site inside them. For the script (rs.cleanup false) this is a
// no-op; the script's outcome is classified from its own endings and
// faults.
//
// Two errors are not evidence. errEnded is the engine declining to touch
// the machine after the run has already ended, not the machine refusing.
// And a read's error is exempt at read's own dispatch entry, because a
// read cannot change the machine: a cleanup may probe, catch the refusal,
// and carry on — that is how a script expresses an expected failure in a
// cleanup block. A read inside a wait_until is not exempt, because its
// error surfaces at the wait's dispatch entry, not read's: the wait was
// abandoned, so the work the block meant to do after it never ran. The
// boundary sees only "a failure surfaced at this entry", and which entry
// the script called is exactly the distinction between the two.
//
// The where names the fault the operation itself recorded, when it
// recorded one for this very error (a refused read inside the wait, a
// refused write, a refused parse); an error that arrived without any
// recording — the missing predicate, a timeout the operation authored —
// is named by the entry the block called.
func (rs *runState) cleanupEvidence(op string, err error) {
	if err == nil || !rs.cleanup || errors.Is(err, errEnded) {
		return
	}
	where := op
	if rs.opErr != nil && errors.Is(err, rs.opErr) {
		where = rs.opWhere
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

	// Transient: the fault could resolve on its own and the cleanup left the machine
	// ready, so the message is safe to run again unchanged.
	Transient

	// Persistent is emitted when either the fault will not resolve by repeating, or
	// the cleanup did not finish, so the machine's state is unknown.
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
	// Nobody means an hour. A handshake is one machine's acknowledgement of
	// one instruction, measured in seconds; an hour-long budget is a unit
	// mistake -- milliseconds typed as microseconds, or a value carried from
	// somewhere that meant something else -- and the cost of accepting it is
	// a message that sits on a machine for an hour before anyone learns it
	// failed. Refusing at config time is the only place this is cheap: once
	// the pipeline is running, the symptom is a bridge that looks healthy
	// and delivers nothing (Jeremy, 2026-11-26).
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
	// block without a budget hangs Run and Close on a machine that never
	// answers — reachable from an omitted YAML field alone (round-2
	// review, item 3). Either block may be empty; a non-empty one must be
	// budgeted, and the refusal names both.
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
	blocks := make(map[string]*goja.Program, 3)
	for _, block := range []struct{ name, source string }{
		{"script", c.Script}, {"onSuccess", c.OnSuccess}, {"onError", c.OnError},
	} {
		program, err := compile(block.name, block.source)
		if err != nil {
			return nil, err
		}
		blocks[block.name] = program
	}
	// LIB-9, literal waits only, and only live ones: a script whose waits
	// add up to more than its own limit is killed on a slow day, mid-commit.
	// An early friendly error; the safety mechanism is wait_until clamping
	// its timeout to the deadline. Every block's waits are summed against
	// that block's own budget, comments are not code, and a backtick
	// template literal no longer desynchronises the scanner (round-2
	// review, item 4).
	for _, block := range []struct {
		name, budgetName, source string
		budget                   time.Duration
	}{
		{"script", "MaxScriptRuntime", c.Script, c.MaxScriptRuntime},
		{"onSuccess", "MaxOnSuccessRuntime", c.OnSuccess, c.MaxOnSuccessRuntime},
		{"onError", "MaxOnErrorRuntime", c.OnError, c.MaxOnErrorRuntime},
	} {
		waits := literalWaitTotal(block.source)
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
// script sees it. It must not be called concurrently with itself on the same Engine.
// Canceling ctx ends the run as a deadline does; OnError still runs.
func (e *Engine[UnifiedAddress]) Run(ctx context.Context, msg map[string]any) Result {
	e.mu.Lock()
	defer e.mu.Unlock()

	// The script's own deadline, which a canceled caller ends the same way.
	// It stops a runaway, not paces a machine (SCR-4).
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
	// deadline stopped (LIB-2). The block runs on its own clock, not the
	// caller's, so a canceled parent does not stop the machine being handed
	// back; and the cleanup blocks see nothing but the machine.
	block, budget := "onError", e.cfg.MaxOnErrorRuntime
	if result.Outcome == Delivered {
		block, budget = "onSuccess", e.cfg.MaxOnSuccessRuntime
	}
	if err := e.runCleanup(block, budget); err != nil {
		// LIB-11: a delivery stands, carrying the cleanup failure beside it.
		// LIB-4: anything else is persistent, whatever the fault was.
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
// error — caught or not — because in both the machine was not returned to
// a state it can be asked from (LIB-4). Which operations failed, and where
// inside them, is not this function's business: the evidence is recorded
// at the dispatch boundary (cleanupEvidence), so this only reads it.
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
		// The block's JavaScript ended cleanly, but an operation whose
		// error it caught — or never saw, because it returned before the
		// machine was touched — did not do what the block asked for, so
		// the machine was not handed back however politely the block
		// carried on.
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

// literalWaitTotal sums the literal timeouts of a block's wait_until calls.
// A timeout that is not a plain number is computed, hence a person's to check
// (SCR-4), and counts as nothing rather than refusing what it cannot read.
// Waits inside comments are not code and are not counted.
func literalWaitTotal(source string) time.Duration {
	total := time.Duration(0)
	for _, args := range waitCalls(stripComments(source)) {
		if len(args) < 3 {
			continue
		}
		ms, ok := literalMillis(strings.TrimSpace(args[2]))
		if !ok {
			continue
		}
		// Saturate, never wrap. literalMillis refuses a literal too big for
		// int64 MILLISECONDS, but the sum is a time.Duration -- int64
		// NANOSECONDS -- so a literal that merely fits overflows here:
		// 9223372036855 made the total negative, the budget check passed,
		// and a five-second wait was deployed under a one-second budget with
		// no complaint. A wait this large is over every budget, which is the
		// answer the check wants, so saturating gives it that answer.
		if ms > maxLiteralMillis {
			return maxLiteralWait
		}
		d := time.Duration(ms) * time.Millisecond
		if total > maxLiteralWait-d {
			return maxLiteralWait
		}
		total += d
	}
	return total
}

const (
	// maxLiteralWait is what the scanner reports for a literal, or a sum,
	// that it cannot hold. It is larger than any budget, so it always loses
	// the comparison it exists for.
	maxLiteralWait = time.Duration(math.MaxInt64)
	// maxLiteralMillis is the largest millisecond literal that converts to a
	// Duration without overflowing.
	maxLiteralMillis = int64(maxLiteralWait / time.Millisecond)
)

// stripComments removes // and /* */ comments, leaving strings intact,
// so the literal-wait scanner does not count a wait that is commented out.
// A regex literal that contains // would be misread as a comment start; a
// handshake script has no reason for one, and the scanner is the friendly
// early error, not the safety mechanism, so it is not armored further.
func stripComments(script string) string {
	var b strings.Builder
	for i := 0; i < len(script); {
		switch {
		case strings.HasPrefix(script[i:], "//"):
			nl := strings.IndexByte(script[i:], '\n')
			if nl < 0 {
				return b.String() // comment to the end; nothing live remains
			}
			i += nl // keep the newline: it ends the comment
		case strings.HasPrefix(script[i:], "/*"):
			end := strings.Index(script[i:], "*/")
			if end < 0 {
				return b.String()
			}
			i += end + 2
		case script[i] == '"' || script[i] == '\'' || script[i] == '`':
			// String contents are LIVE TEXT to this scanner, deliberately.
			// Scanning them counts a wait inside a string literal, which
			// can refuse a deploy for a script that has no waits -- but
			// NOT scanning them would miss a real wait hidden by a regex
			// literal that desyncs the quote tracking, and for a check
			// whose job is to catch a wait that cannot fit, a false refusal
			// is the safer error than a miss. Pinned by
			// TestLiteralWaitScanner_AnEscapedQuoteDoesNotEndAString and
			// its neighbor. Open on ENG-8129 if that trade is wrong.
			end := skipString(script, i)
			b.WriteString(script[i : end+1])
			i = end + 1
		default:
			b.WriteByte(script[i])
			i++
		}
	}
	return b.String()
}

// waitCalls finds every wait_until( in a script and returns its arguments,
// split on the commas that sit outside brackets and strings.
func waitCalls(script string) [][]string {
	const call = "wait_until"
	var calls [][]string
	for i := 0; ; {
		at := strings.Index(script[i:], call)
		if at < 0 {
			return calls
		}
		name := i + at
		open := name + len(call)
		// The name has to stand alone: mywait_until is somebody else's
		// function and counting it refused a deploy that had no waits.
		if name > 0 && isIdentRune(lastRune(script[:name])) {
			i = open
			continue
		}
		// JavaScript allows space before the parenthesis, and missing those
		// let a real wait through the friendly error entirely.
		for open < len(script) && (script[open] == ' ' || script[open] == '\t' ||
			script[open] == '\n' || script[open] == '\r') {
			open++
		}
		if open >= len(script) || script[open] != '(' {
			i = name + len(call)
			continue
		}
		if end, args, ok := splitCall(script, open); ok {
			calls = append(calls, args)
			i = end
		} else {
			i = open + 1
		}
	}
}

// isIdentRune reports whether r can be part of a JavaScript identifier, so
// the scanner can tell a call to wait_until from the tail of another name.
func isIdentRune(r rune) bool {
	// unicode.IsLetter and not an ASCII range: goja compiles identifiers
	// like ewait_until with an accent, so a call to a user's own function
	// was counted as a wait_until and refused a deploy that had no waits in
	// it at all. Reading the rune rather than the byte before the name is
	// the other half -- a continuation byte matched nothing.
	return r == '_' || r == '$' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// lastRune is the final rune of s, or -1 when s is empty. utf8 decoding and
// not s[len(s)-1], which for a multi-byte rune is a continuation byte.
func lastRune(s string) rune {
	if s == "" {
		return -1
	}
	r, _ := utf8.DecodeLastRuneInString(s)
	return r
}

// splitCall takes the index of a '(' and splits the call's arguments on the
// commas that sit outside every bracket and string, up to the matching ')'.
func splitCall(script string, open int) (int, []string, bool) {
	depth, start := 0, open+1
	for i := open; i < len(script); i++ {
		switch script[i] {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
			if depth == 0 {
				return i, splitCommas(script[start:i]), true
			}
		case '"', '\'', '`':
			i = skipString(script, i)
		}
	}
	return 0, nil, false
}

// splitCommas splits on the commas at bracket depth zero.
func splitCommas(s string) []string {
	var args []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case '"', '\'', '`':
			i = skipString(s, i)
		case ',':
			if depth == 0 {
				args = append(args, s[start:i])
				start = i + 1
			}
		}
	}
	return append(args, s[start:])
}

// skipString returns the index of the closing quote of the string starting at i.
func skipString(s string, i int) int {
	quote := s[i]
	for i++; i < len(s); i++ {
		if s[i] == '\\' {
			i++
			continue
		}
		if s[i] == quote {
			return i
		}
	}
	return len(s) - 1
}

// literalMillis reads a plain integer literal; anything else is computed and
// beyond the check.
func literalMillis(arg string) (int64, bool) {
	// Numeric separators are part of the literal, not of the number, and a
	// 1_000 the scanner could not read was counted as nothing -- a real
	// one-second wait passing a 999ms budget unseen.
	arg = strings.ReplaceAll(arg, "_", "")
	if arg == "" {
		return 0, false
	}
	// Hex is a plain number too, and 0x3E8 slipped through the same gap.
	base, digits := int64(10), arg
	if len(arg) > 2 && arg[0] == '0' && (arg[1] == 'x' || arg[1] == 'X') {
		base, digits = 16, arg[2:]
	}
	// A unary plus is applied to a literal, not computed from one, and a
	// script that writes +5000 means 5000.
	if base == 10 && len(digits) > 1 && digits[0] == '+' {
		digits = digits[1:]
	}
	// A plain number with a decimal point or an exponent is still a plain
	// number: 5000.0, 5e3 and .5e4 are all the same five-second wait, and
	// all three were read as "computed" and counted as nothing -- the
	// identical false pass that hex and 1_000 were fixed for, in four more
	// spellings. Only these characters, so a name or an expression still
	// falls through to the "computed" answer.
	if base == 10 && strings.ContainsAny(digits, ".eE") {
		f, err := strconv.ParseFloat(digits, 64)
		if err != nil || f < 0 || math.IsNaN(f) {
			return 0, false
		}
		if f >= float64(maxLiteralMillis) {
			return math.MaxInt64, true
		}
		return int64(f), true
	}
	var n int64
	for _, r := range digits {
		d := int64(-1)
		switch {
		case r >= '0' && r <= '9':
			d = int64(r - '0')
		case base == 16 && r >= 'a' && r <= 'f':
			d = int64(r-'a') + 10
		case base == 16 && r >= 'A' && r <= 'F':
			d = int64(r-'A') + 10
		}
		if d < 0 {
			return 0, false // not a plain number: computed, a person's to check
		}
		// Saturate rather than wrap, and saturate rather than give up: a
		// literal too large for int64 is not a wait anyone meant, so the
		// scanner must refuse the deploy over it instead of counting it as
		// nothing. Letting the multiply wrap made the running total
		// NEGATIVE, which masked every real wait beside it.
		if n > (math.MaxInt64-d)/base {
			return math.MaxInt64, true
		}
		n = n*base + d
	}
	return n, true
}
