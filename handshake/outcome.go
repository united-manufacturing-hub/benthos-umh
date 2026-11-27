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
	"encoding/json"
	"errors"
	"fmt"
)

// classify turns how the script ended into the outcome half of a Result.
// Whether the machine got the data is decided here; the cleanup that runs
// afterwards can only make a failure persistent.
//
// The order of the cases is the precedence. A run whose own clock ran out is
// never a delivery, however it ended on the way: the ending functions abort
// by throwing, a throw is catchable, and an ending decides the outcome only
// when the script stopped for it. Below that, an explicit ending outranks a
// binding fault, because a script that reached an ending caught whatever
// failed and chose how to finish. A fault decides the outcome on its own
// only when the script did not catch it, that is, when the run's error
// carries it.
func classify(ctx context.Context, rs *runState, runErr error) Result {
	switch {
	case ctx.Err() != nil:
		// The run's own clock ran out, so the script never finished. A
		// script that catches an ending carries on until the clock stops
		// it, and a caught success must not run onSuccess on a transaction
		// cut in half. A deadline that arrives in the same instant as a
		// real ending resolves here too, to the conservative side. An
		// unmarked operation fault that surfaced as the clock ran out is
		// part of the same stop, not a retryable machine fault.
		what := "script stopped before it ended"
		if rs.end != endNone {
			what += " after an ending it did not stop for"
		}
		if rs.opErr != nil {
			return failed(Persistent, fmt.Errorf(
				"%s (%s failed: %w): %w", what, rs.opWhere, rs.opErr, ctx.Err()))
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
	case runErr != nil:
		// Something stopped the script. If it was an operation's fault,
		// the fault travels with the exception, uncaught or rethrown, and
		// errors.As finds it. A fault the script caught and carried on from
		// is not in the run's error, so it cannot decide the class: a script
		// that caught a flaky read and then hit a bug of its own is that
		// bug, not a retryable machine fault.
		var f *fault
		if errors.As(runErr, &f) {
			// The outcome follows the fault, not whether the script
			// returned it or a call threw it.
			if errors.Is(f.err, ErrPersistent) {
				return failed(Persistent, fmt.Errorf("%s failed: %w", f.where, f.err))
			}
			return failed(Transient, fmt.Errorf("%s failed: %w", f.where, f.err))
		}
		// A script fault: an uncaught exception, or a call to a name the
		// protocol does not offer. A fault the script caught does not
		// explain this, and saying so would send someone to the network.
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
	// JSON, not %v: a script can pass an object that contains itself, and
	// %v recurses on it until the Go runtime dies, which recover cannot stop.
	if text, ok := detail.(string); ok {
		return fmt.Errorf("%s: %s", what, text)
	}
	b, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("%s: <detail that cannot be rendered: %w>", what, err)
	}
	return fmt.Errorf("%s: %s", what, b)
}
