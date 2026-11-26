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

// Three int64 overflows, each of which turned a refusal into a pass. They
// share a shape: a number large enough to wrap reads as a number small
// enough to allow, so the check that exists to catch absurd input is the
// one the absurd input walks through.

// The scanner's sum is nanoseconds, so a literal that fits int64
// MILLISECONDS still overflows it. 9223372036855 is 2^63 nanoseconds
// expressed in milliseconds: one digit shorter than the literal the digit
// guard refuses, and it carried a real five-second wait past a one-second
// budget with no complaint.
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

// The same overflow at runtime, one layer down: waitUntil converted the
// script's timeout to a Duration BEFORE comparing it to MaxInterval, and
// the conversion is where it wrapped. Which way it wraps is per
// architecture -- arm64 saturates to int64 max, amd64 turns max into min --
// so on one of them the wrapped value is negative and loses the comparison.
func TestWaitUntil_ATimeoutTooLargeForADurationIsRefusedNotWrapped(t *testing.T) {
	// Written as expressions, not literals: a plain literal this large is
	// now refused by the scanner at config time, which is the fix above.
	// These are what reaches waitUntil when the number is computed, the
	// case the scanner deliberately does not read.
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

// The fault slice exists so classify can find the fault the run's error
// actually IS, which is the LAST one recorded. A cap that kept the first 32
// dropped exactly that one: with 31 caught faults the stopper was recorded
// and the run was Transient naming the node, with 32 it was dropped and the
// identical machine fault became Persistent with a raw goja dump for an
// error -- the degradation the fault slice was added to stop. A retry loop
// around a flaky node is ordinary defensive scripting, so 32 is not an
// absurd count.
func TestClassify_TheCapKeepsTheFaultThatStoppedTheRun(t *testing.T) {
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
