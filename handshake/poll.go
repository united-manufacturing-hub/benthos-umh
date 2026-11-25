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
func (e *Engine[UnifiedAddress]) waitUntil(
	rs *runState, raw map[string]any, pred func(any) bool,
	timeoutMs float64, opts map[string]any,
) (any, error) {
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
			return nil, err // recorded, and attempted once (LIB-8)
		}
		if pred(value) {
			return value, nil
		}
		elapsed := time.Since(start)
		if elapsed >= timeout {
			if rs.finish(endWaitTimeout, describe) {
				rs.waitPersistent = persistent
			}
			return nil, fmt.Errorf("wait_until timed out after %v on %s",
				elapsed.Round(time.Millisecond), describe)
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
