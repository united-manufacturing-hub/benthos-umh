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
	"math"
	"time"
)

// waitUntil re-reads one address until the predicate holds or the wait's
// timeout runs out. The timeout is clamped to the time remaining before the
// block's deadline, so the deadline never fires mid-transaction, which would
// make the run persistent. A wait that runs out ends the run transient,
// unless the call passed { onTimeout: "persistent" }. Inside a cleanup
// block, which has no ending, a failed wait is recorded as the cleanup's
// failure instead (cleanupEvidence).
func (e *Engine[UnifiedAddress]) waitUntil(
	rs *runState, raw map[string]any, pred func(any) bool,
	timeoutMs float64, opts map[string]any,
) (any, error) {
	// A wait without its predicate is a wrong script. It is persistent,
	// so the fault propagates as an exception rather than being recorded
	// as a binding fault.
	if pred == nil {
		return nil, fmt.Errorf(
			"wait_until needs a predicate as its second argument, such as v => v === true")
	}
	// A wait after the run has ended is refused like every other operation:
	// a wait that timed out is itself an ending, so a script that catches
	// it cannot start another one under the ended transaction.
	addr, err := e.address(rs, raw)
	if err != nil {
		return nil, err
	}
	describe := e.bind.Describe(addr)

	// A wait_until watches a machine confirm one instruction, so a timeout
	// over MaxInterval is a unit mistake far more often than an intention,
	// and taking it at face value holds a message on a machine for an hour
	// while the bridge reports healthy. A wrong script is persistent:
	// repeating it will not make the number smaller.
	//
	// Compared as milliseconds and not as a Duration. Converting first lets
	// a wildly large number overflow int64 nanoseconds, and the product then
	// wraps past this very check -- and which way it wraps is per
	// architecture (arm64 saturates to max where amd64 turns max into min),
	// so the comparison has to happen before the conversion. The check is
	// written as a negated <= so that it also refuses a NaN, but a NaN is
	// refused first with its own message: it is a computed timeout gone
	// wrong (0/0, a missing field times 1000), not a long one.
	if math.IsNaN(timeoutMs) {
		return nil, fmt.Errorf("wait_until on %s was given a timeout that is not a number (NaN)", describe)
	}
	if timeoutMs < 0 {
		return nil, fmt.Errorf("wait_until on %s was given a negative timeout of %v ms", describe, timeoutMs)
	}
	if !(timeoutMs <= maxIntervalMillis) {
		return nil, fmt.Errorf(
			"wait_until on %s was given a timeout of %.0f ms, longer than the %v a handshake is allowed: check the units -- this is milliseconds",
			describe, timeoutMs, MaxInterval)
	}

	poll := e.cfg.PollInterval
	if poll <= 0 {
		poll = 100 * time.Millisecond // the documented default
	}
	poll = max(poll, time.Millisecond) // a sub-millisecond interval is no pause at all
	if ms, ok := number(opts["pollMs"]); ok && ms > 0 {
		// Bounded before the conversion, for the same reason the timeout
		// above is. time.Duration(1e13) * time.Millisecond wraps negative,
		// and a negative poll interval means no pause between reads, so the
		// wait would read the machine at wire speed until its timeout, which
		// may be up to an hour. The outcome would stay right, because the
		// deadline clamp holds; the damage is to the PLC and its network.
		if !(ms <= maxIntervalMillis) {
			return nil, fmt.Errorf(
				"wait_until on %s was given a pollMs of %.0f, longer than the %v a handshake is allowed: check the units -- this is milliseconds",
				describe, ms, MaxInterval)
		}
		// Below 1ms the conversion truncates to no pause at all, which reads
		// the machine at wire speed; 1ms is the floor.
		poll = max(time.Duration(ms*float64(time.Millisecond)), time.Millisecond)
	}
	persistent, err := waitOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("wait_until on %s %w", describe, err)
	}

	timeout := time.Duration(timeoutMs) * time.Millisecond
	if !rs.deadline.IsZero() {
		if remaining := time.Until(rs.deadline); remaining < timeout {
			timeout = clampWait(remaining)
		}
	}

	start := time.Now()
	// Each read is bounded by the wait's own end, not the run's deadline,
	// so a read that starts near the end and overruns ends the wait as a
	// transient timeout rather than letting the run's deadline stop the
	// script, which would be persistent. The bound is exact; skipping a
	// read because the previous one was slow would end a wait early on a
	// machine whose first read after a connect is slow and whose second
	// read is ready.
	waitEnds := start.Add(timeout)
	for {
		value, cutOff, err := e.readWithin(rs, raw, waitEnds)
		if cutOff {
			// The machine did not answer within the time the wait had.
			return nil, e.waitRanOut(rs, describe, time.Since(start), persistent)
		}
		if err != nil {
			// readWithin recorded the refused read. The read is not
			// retried; inside a cleanup block, the dispatch boundary
			// records it as the cleanup's failure (cleanupEvidence).
			return nil, err
		}
		if pred(value) {
			return value, nil
		}
		elapsed := time.Since(start)
		if elapsed >= timeout {
			return nil, e.waitRanOut(rs, describe, elapsed, persistent)
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
// never on it: a deadline that fires mid-transaction is persistent, and the
// clamp exists so it never does.
func clampWait(remaining time.Duration) time.Duration {
	const margin = 50 * time.Millisecond
	if remaining <= margin {
		return remaining / 2
	}
	return remaining - margin
}

// waitRanOut ends a wait that did not see what it was waiting for. In the
// script it records the wait_until ending.
//
// A cleanup has no ending to record, and setting rs.end there would refuse
// every operation after the throw as errEnded, before the block could hand
// the machine back. The dispatch boundary records the timeout as the
// cleanup's failure instead (cleanupEvidence), and the block carries on:
// a wait-for-idle followed by a reset has the reset as its point. The
// recorded failure still makes the run persistent, so a human looks at the
// machine.
func (e *Engine[UnifiedAddress]) waitRanOut(
	rs *runState, describe string, elapsed time.Duration, persistent bool,
) error {
	err := fmt.Errorf("wait_until timed out after %v on %s",
		elapsed.Round(time.Millisecond), describe)
	if rs.cleanup {
		return err
	}
	if rs.finish(endWaitTimeout, describe) {
		rs.waitPersistent = persistent
	}
	return err
}

// waitOptions checks wait_until's options object and reports whether a wait
// that runs out ends persistent. An unknown option or onTimeout value is a
// wrong script, refused rather than read as the default, which would make a
// timeout silently transient. Validate applies the same check to literal
// options at deploy time (checkWaitOptions).
func waitOptions(opts map[string]any) (bool, error) {
	for key := range opts {
		if err := waitOptionKey(key); err != nil {
			return false, err
		}
	}
	return onTimeoutValue(opts["onTimeout"])
}

// waitOptionKey refuses an option wait_until does not take.
func waitOptionKey(key string) error {
	switch key {
	case "pollMs", "onTimeout":
		return nil
	default:
		return fmt.Errorf("was given the unknown option %q: it takes pollMs and onTimeout", key)
	}
}

// onTimeoutValue reads the onTimeout option; absent is transient.
func onTimeoutValue(v any) (bool, error) {
	switch v {
	case nil, "transient":
		return false, nil
	case "persistent":
		return true, nil
	default:
		return false, fmt.Errorf(`was given onTimeout %#v: it must be "transient" or "persistent"`, v)
	}
}
