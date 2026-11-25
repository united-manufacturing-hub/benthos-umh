package handshake

import (
	"context"
	"fmt"
	"strings"
	"time"
)

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

// Validate reports whether the configuration can run. It compiles all three blocks, so
// a script that does not parse is caught at deploy time rather than against a machine.
func (c Config) Validate() error {
	if c.MaxScriptRuntime <= 0 {
		return fmt.Errorf("MaxScriptRuntime must be positive, is %v", c.MaxScriptRuntime)
	}
	rt := newRuntime()
	defer func() { _ = rt.Close() }()
	for _, block := range []struct{ name, source string }{
		{"script", c.Script}, {"onSuccess", c.OnSuccess}, {"onError", c.OnError},
	} {
		if _, err := rt.Compile(block.name, block.source); err != nil {
			return err
		}
	}
	// LIB-9, literal waits only: a script whose waits add up to more than its
	// own limit is killed on a slow day, mid-commit. An early friendly error;
	// the safety mechanism is wait_until clamping its timeout to the deadline.
	waits := literalWaitTotal(c.Script)
	if waits > c.MaxScriptRuntime {
		return fmt.Errorf(
			"script's literal waits total %v, which exceeds MaxScriptRuntime %v",
			waits, c.MaxScriptRuntime)
	}
	return nil
}

// An Engine runs one configured handshake against one machine, once per message.
type Engine[UnifiedAddress any] struct {
	bind Binding[UnifiedAddress]
	cfg  Config
	_    [0]func() // not comparable, and not copyable by value
}

// New compiles the configuration and returns an Engine bound to one binding. It returns
// an error only for a configuration that cannot run; everything that can go wrong later
// is reported per run, in a Result.
func New[UnifiedAddress any](
	cfg Config, p Binding[UnifiedAddress],
) (*Engine[UnifiedAddress], error) {
	panic("skeleton")
}

// Run executes the handshake once for one message, where msg is the message as the
// script sees it. It must not be called concurrently with itself on the same Engine.
// Cancelling ctx ends the run as a deadline does; OnError still runs.
func (e *Engine[UnifiedAddress]) Run(ctx context.Context, msg map[string]any) Result {
	panic("skeleton")
}

// Close releases the runtimes the Engine pooled. A Run already in flight finishes first.
func (e *Engine[UnifiedAddress]) Close() error { panic("skeleton") }

// literalWaitTotal sums the literal timeouts of a script's wait_until calls.
// A timeout that is not a plain number is computed, hence a person's to check
// (SCR-4), and counts as nothing rather than refusing what it cannot read.
func literalWaitTotal(script string) time.Duration {
	total := time.Duration(0)
	for _, args := range waitCalls(script) {
		if len(args) < 3 {
			continue
		}
		ms, ok := literalMillis(strings.TrimSpace(args[2]))
		if !ok {
			continue
		}
		total += time.Duration(ms) * time.Millisecond
	}
	return total
}

// waitCalls finds every wait_until( in a script and returns its arguments,
// split on the commas that sit outside brackets and strings.
func waitCalls(script string) [][]string {
	const call = "wait_until("
	var calls [][]string
	for i := 0; ; {
		at := strings.Index(script[i:], call)
		if at < 0 {
			return calls
		}
		open := i + at + len(call) - 1 // the '(' of the call
		if end, args, ok := splitCall(script, open); ok {
			calls = append(calls, args)
			i = end
		} else {
			i = open + 1
		}
	}
}

// splitCall takes the index of a '(' and splits the call's arguments on the
// commas that sit outside every bracket and string, up to the matching ')'.
func splitCall(script string, open int) (end int, args []string, ok bool) {
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
		case '"', '\'':
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
		case '"', '\'':
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
	if arg == "" {
		return 0, false
	}
	var n int64
	for _, r := range arg {
		if r < '0' || r > '9' {
			return 0, false
		}
		n = n*10 + int64(r-'0')
	}
	return n, true
}
