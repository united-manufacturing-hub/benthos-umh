package handshake

import (
	"context"
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
func (c Config) Validate() error { panic("skeleton") }

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
