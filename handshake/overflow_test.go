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
	"strings"
	"testing"
	"time"
)

// The int64 overflow tests share a shape: a number large enough to wrap
// would read as a number small enough to allow, so the check that exists to
// catch absurd input would be the one the absurd input walks through.

// The literal-wait sum is nanoseconds, so a literal that fits int64
// milliseconds can still overflow it. 9223372036855 is 2^63 nanoseconds
// expressed in milliseconds; wrapped, it would carry a real five-second wait
// past a one-second budget with no complaint.
func TestValidate_ALiteralThatFitsInt64MillisecondsCannotHideARealWait(t *testing.T) {
	cfg := Config{
		Script: `opcua.wait_until({nodeId: "ns=2;s=A"}, v => v === true, 5000);` +
			`opcua.wait_until({nodeId: "ns=2;s=B"}, v => v === true, 9223372036855);` +
			`return opcua.success({});`,
		MaxScriptRuntime: time.Second,
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("a five-second literal wait was accepted under a one-second budget, because the wait beside it wrapped the total negative")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("refused for the wrong reason: %v", err)
	}
}

// The same overflow at run time: waitUntil compares the script's timeout to
// MaxInterval before converting it to a Duration, because the conversion is
// where it would wrap. Which way it wraps is per architecture -- arm64
// saturates to int64 max, amd64 turns max into min -- so on one of them the
// wrapped value is negative and would pass the comparison.
func TestWaitUntil_ATimeoutTooLargeForADurationIsRefusedNotWrapped(t *testing.T) {
	// Written as expressions, not literals: a plain literal this large is
	// refused by Validate at config time, as the test above shows. These
	// are what reaches waitUntil when the number is computed, which
	// Validate deliberately does not read.
	for _, ms := range []string{"1e19 * 10", "9223372036855 * 1", "Number.MAX_VALUE"} {
		t.Run(ms, func(t *testing.T) {
			b := &fakeBinding{values: map[string]any{"ns=2;s=A": false}}
			engine := newTestEngine(t, b, func(c *Config) {
				c.MaxScriptRuntime = 2 * time.Second
				c.Script = fmt.Sprintf(
					`opcua.wait_until({nodeId: "ns=2;s=A"}, v => v === true, %s); return opcua.success({});`, ms)
			})
			start := time.Now()
			res := engine.Run(t.Context(), map[string]any{})
			if res.Outcome == Delivered {
				t.Fatalf("a timeout of %s ms was accepted", ms)
			}
			if !strings.Contains(res.Err.Error(), "longer than the") {
				t.Fatalf("refused for the wrong reason: %v", res.Err)
			}
			// It must be refused rather than waited out: a wrapped value
			// that happened to stay positive would park the message for
			// however long the wrap produced.
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Errorf("the refusal took %v, so the timeout was waited on rather than refused", elapsed)
			}
		})
	}
}

// However many faults the script caught first, the fault that stopped the
// run decides its class and names its node. A retry loop around a flaky
// node is ordinary defensive scripting, so dozens of caught faults are not
// an absurd count.
func TestClassify_ManyCaughtFaultsDoNotDisplaceTheFaultThatStoppedTheRun(t *testing.T) {
	for _, caught := range []int{5, 31, 32, 40} {
		t.Run(fmt.Sprintf("%d caught before the stopper", caught), func(t *testing.T) {
			b := &fakeBinding{
				values: map[string]any{"ns=2;s=Flaky": true, "ns=2;s=Stopper": true},
				errs: map[string]error{
					"read:ns=2;s=Flaky":   fmt.Errorf("machine unreachable"),
					"read:ns=2;s=Stopper": fmt.Errorf("machine unreachable"),
				},
			}
			engine := newTestEngine(t, b, func(c *Config) {
				c.MaxScriptRuntime = 5 * time.Second
				c.Script = fmt.Sprintf(`
					for (let i = 0; i < %d; i++) {
						try { opcua.read({nodeId: "ns=2;s=Flaky"}); } catch (e) { }
					}
					opcua.read({nodeId: "ns=2;s=Stopper"});
					return opcua.success({});
				`, caught)
			})

			res := engine.Run(t.Context(), map[string]any{})
			if res.Outcome != Transient {
				t.Fatalf("outcome %v (want Transient): a machine that cannot be reached is retryable however often the script recovered first; err=%v",
					res.Outcome, res.Err)
			}
			if !strings.Contains(res.Err.Error(), "ns=2;s=Stopper") {
				t.Fatalf("the error does not name the node that stopped the run: %v", res.Err)
			}
		})
	}
}

// The same overflow in pollMs. time.Duration(1e13) * time.Millisecond wraps
// negative, a negative poll interval is no pause at all, and the wait would
// read the machine as fast as the wire allows until its timeout runs out.
// The outcome would still be right, because the deadline clamp holds, so
// only the read count shows it; the damage is what a real PLC and its
// network see, and the timeout it floods for may be up to an hour.
func TestWaitUntil_APollIntervalTooLargeForADurationIsRefusedNotWrapped(t *testing.T) {
	for _, pollMs := range []string{"1e13", "1.1e13", "1e15", "Number.MAX_VALUE"} {
		t.Run(pollMs, func(t *testing.T) {
			b := &fakeBinding{values: map[string]any{"ns=2;s=A": false}}
			engine := newTestEngine(t, b, func(c *Config) {
				c.MaxScriptRuntime = 2 * time.Second
				c.Script = fmt.Sprintf(
					`opcua.wait_until({nodeId: "ns=2;s=A"}, v => v === true, 300, {pollMs: %s}); return opcua.success({});`,
					pollMs)
			})
			res := engine.Run(t.Context(), map[string]any{})
			if res.Outcome == Delivered {
				t.Fatalf("a pollMs of %s was accepted", pollMs)
			}
			if !strings.Contains(res.Err.Error(), "longer than the") {
				t.Fatalf("refused for the wrong reason: %v", res.Err)
			}
			// The refusal must happen before any reading, not after a
			// flood that the wait's own end then cut short.
			if n := b.count("read ns=2;s=A"); n > 1 {
				t.Errorf("the machine was read %d times before the pollMs was refused", n)
			}
		})
	}

	// A sane pollMs still works, so the bound is a bound and not a ban.
	b := &fakeBinding{values: map[string]any{"ns=2;s=A": false}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.MaxScriptRuntime = 2 * time.Second
		c.Script = `opcua.wait_until({nodeId: "ns=2;s=A"}, v => v === true, 300, {pollMs: 50}); return opcua.success({});`
	})
	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Transient {
		t.Fatalf("a 50ms poll should wait and then time out transiently, got %v (%v)", res.Outcome, res.Err)
	}
	if n := b.count("read ns=2;s=A"); n < 2 || n > 50 {
		t.Errorf("a 300ms wait polling every 50ms read the machine %d times", n)
	}
}
