package handshake

import (
	"context"
	"errors"
	"fmt"
)

// classify turns how the script ended into the outcome half of a Result.
// Whether the machine got the data was decided when the script ended (LIB-3);
// only whether a failure may be repeated waits for the cleanup (LIB-4). The
// order of the cases is the precedence: a fault's kind outranks how the script
// reported it, and a run whose own clock ran out is never a delivery — an
// ending only decides the outcome when the script actually stopped for it,
// because the ending functions abort by throwing and a throw is catchable
// (UAT-13, round 2).
func classify(rs *runState, ctx context.Context, runErr error) Result {
	switch {
	case rs.opErr != nil && errors.Is(rs.opErr, ErrPersistent):
		// LIB-5: the outcome follows the fault, not whether the script
		// returned it or a call threw it.
		return failed(Persistent, fmt.Errorf("%s failed: %w", rs.opWhere, rs.opErr))
	case ctx.Err() != nil:
		// LIB-6 and LIB-2: the run's own clock ran out, so the script never
		// finished — however it ended on the way. An ending only decides the
		// outcome when the script actually stopped for it: the ending
		// functions abort by throwing, a throw is catchable, and a script
		// that catches one carries on until something stops it (UAT-13,
		// round 2, which watched a caught success outrank the deadline and
		// run onSuccess on a cut-in-half transaction). A deadline that
		// arrives in the same instant as a genuine ending resolves here
		// too, to the conservative side. An unmarked operation fault that
		// surfaced as the clock ran out is part of the same stop, not a
		// retryable machine fault (UAT-4).
		what := "script stopped before it ended"
		if rs.end != endNone {
			what += " after an ending it did not stop for"
		}
		if rs.opErr != nil {
			return failed(Persistent, fmt.Errorf(
				"%s (%s failed: %v): %w", what, rs.opWhere, rs.opErr, ctx.Err()))
		}
		return failed(Persistent, fmt.Errorf("%s: %w", what, ctx.Err()))
	case rs.end == endSuccess:
		return Result{Outcome: Delivered, Data: rs.data}
	case rs.end == endNotReady:
		return failed(Transient, detailErr("machine not ready", rs.detail))
	case rs.end == endTransient:
		return failed(Transient, detailErr("script reported a transient fault", rs.detail))
	case rs.end == endPersistent:
		return failed(Persistent, detailErr("script reported a persistent fault", rs.detail))
	case rs.end == endWaitTimeout:
		what := detailErr("wait_until timed out", rs.detail)
		if rs.waitPersistent {
			return failed(Persistent, what)
		}
		return failed(Transient, what)
	case rs.opErr != nil:
		// Unmarked: LIB-5 again, the other half of the rule. A ctx that ran
		// out during the operation is handled above, with the deadline.
		return failed(Transient, fmt.Errorf("%s failed: %w", rs.opWhere, rs.opErr))
	case runErr != nil:
		// A script fault: an uncaught exception, or a call to a name the
		// protocol does not offer.
		return failed(Persistent, runErr)
	default:
		return failed(Persistent, errors.New(
			"script ended without calling success, error_not_ready, error_transient or error_persistent"))
	}
}

// failed builds the Result of a run that did not deliver.
func failed(outcome Outcome, err error) Result {
	return Result{Outcome: outcome, Err: err}
}

// detailErr renders an ending's optional detail argument.
func detailErr(what string, detail any) error {
	if detail == nil {
		return errors.New(what)
	}
	return fmt.Errorf("%s: %v", what, detail)
}
