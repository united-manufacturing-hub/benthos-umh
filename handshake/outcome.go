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
// reported it, and an explicit ending outranks a deadline that arrived late.
func classify(rs *runState, ctx context.Context, runErr error) Result {
	switch {
	case rs.opErr != nil && errors.Is(rs.opErr, ErrPersistent):
		// LIB-5: the outcome follows the fault, not whether the script
		// returned it or a call threw it.
		return failed(Persistent, fmt.Errorf("%s failed: %w", rs.opWhere, rs.opErr))
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
		// Unmarked: LIB-5 again, the other half of the rule.
		return failed(Transient, fmt.Errorf("%s failed: %w", rs.opWhere, rs.opErr))
	case ctx.Err() != nil:
		// LIB-6: the deadline is configuration, so repeating cannot fix it;
		// a cancelled context ends the run the same way.
		return failed(Persistent, fmt.Errorf("script stopped before it ended: %w", ctx.Err()))
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
