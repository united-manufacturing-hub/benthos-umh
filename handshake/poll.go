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
	"fmt"
	"time"
)

// waitUntil re-reads one address until the predicate holds or the clock runs
// out (LIB-12). Its timeout is its own, clamped to the time remaining before
// the block's deadline, so the deadline never fires mid-transaction: LIB-6
// makes that persistent and leaves the machine blocked. A wait that runs out
// ends transient (LIB-18), unless the call passed { onTimeout: "persistent" }.
// Inside a cleanup block the ending functions are withheld, because a
// cleanup has no transaction to end; a wait that fails — ran out of time,
// was refused its read, or could not parse its address — is recorded at
// the dispatch boundary instead (cleanupEvidence), as the cleanup's own
// failure: the state the block waited for never came, so the machine was
// not handed back (LIB-4), caught or not — and the block carries on.
func (e *Engine[UnifiedAddress]) waitUntil(
	rs *runState, raw map[string]any, pred func(any) bool,
	timeoutMs float64, opts map[string]any,
) (any, error) {
	// A wait without its predicate — left off, or a name that is not a
	// function — is a wrong script, not a nil-func call: goja fills the
	// missing argument with a nil func, and calling it panicked straight
	// out of Run with the machine left armed (round-2 review, item 2).
	// A wrong script is persistent, so the fault propagates as an
	// exception rather than being recorded as a binding fault.
	if pred == nil {
		return nil, fmt.Errorf(
			"wait_until needs a predicate as its second argument, such as v => v === true")
	}
	// A wait after the run has ended is refused like every other operation
	// (UAT-13): a wait that timed out is itself an ending, so a script that
	// catches it cannot start another one under the ended transaction.
	if rs.end != endNone {
		return nil, errEnded
	}
	addr, err := e.bind.ParseAddress(raw)
	if err != nil {
		rs.fail("parse address", err)
		return nil, err
	}
	describe := e.bind.Describe(addr)

	poll := e.cfg.PollInterval
	if poll <= 0 {
		poll = 100 * time.Millisecond // the documented default
	}
	if ms, ok := number(opts["pollMs"]); ok && ms > 0 {
		poll = time.Duration(ms) * time.Millisecond
	}
	persistent := false
	if s, ok := opts["onTimeout"].(string); ok {
		persistent = s == "persistent"
	}

	timeout := time.Duration(timeoutMs) * time.Millisecond
	if !rs.deadline.IsZero() {
		if remaining := time.Until(rs.deadline); remaining < timeout {
			timeout = clampWait(remaining)
		}
	}

	start := time.Now()
	for {
		value, err := e.readOp(rs, raw)
		if err != nil {
			// The read is refused and recorded by readOp itself; whether
			// that refusal is the cleanup's failure is not this line's to
			// decide. The dispatch boundary decides it, and it does: the
			// error surfaces at wait_until's own entry, not at read's
			// exempt one, so a cleanup that catches this throw cannot
			// report a handback that never ran (cleanupEvidence).
			// Attempted once (LIB-8).
			return nil, err
		}
		if pred(value) {
			return value, nil
		}
		elapsed := time.Since(start)
		if elapsed >= timeout {
			err := fmt.Errorf("wait_until timed out after %v on %s",
				elapsed.Round(time.Millisecond), describe)
			if rs.cleanup {
				// A cleanup has no ending to record — that is why the
				// ending functions are withheld from it — and setting
				// rs.end here would refuse every operation after the
				// throw as errEnded before the block could hand the
				// machine back: the outcome was decided before the
				// cleanup ran, and the handback is its job. The dispatch
				// boundary records the timeout as the cleanup's failure
				// instead (cleanupEvidence), and the block carries on —
				// a wait-for-idle followed by a reset has the reset as its
				// point — while the recorded failure still makes the run
				// persistent, so a human looks at the machine.
				return nil, err
			}
			if rs.finish(endWaitTimeout, describe) {
				rs.waitPersistent = persistent
			}
			return nil, err
		}
		next := poll
		if left := timeout - elapsed; left < next {
			next = left
		}
		select {
		case <-rs.ctx.Done():
			// A cancellation ends the run as a deadline does; the run is
			// classed from the context, not from here.
			return nil, rs.ctx.Err()
		case <-time.After(next):
		}
	}
}

// number reads a JS number out of an options object, in whichever Go type
// the runtime's reflection handed it back.
func number(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	}
	return 0, false
}

// clampWait leaves a margin so the wait ends strictly before the deadline,
// never on it: a deadline that fires mid-transaction is persistent (LIB-6),
// and the clamp exists so it never does.
func clampWait(remaining time.Duration) time.Duration {
	const margin = 50 * time.Millisecond
	if remaining <= margin {
		return remaining / 2
	}
	return remaining - margin
}
