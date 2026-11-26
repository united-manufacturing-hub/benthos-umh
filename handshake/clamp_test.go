//go:build uat

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
	"testing"
	"time"
)

// A clamped wait ends as a wait, not as a deadline, however slow the machine
// is (LIB-12).
//
// The clamp used to leave a fixed 50ms margin before the run's deadline, and
// a read is only discovered to be too late once it returns. So against a
// machine answering in 60ms the last read started inside the margin, ran
// past the deadline, and the run came back Persistent -- blocking for the
// whole budget instead of returning at the clamped timeout. That is the
// opposite of what the clamp is for, and the shipped test of the clamp could
// not see it because its machine answers in about 0ms.
func TestClampedWait_EndsAsAWaitHoweverSlowTheMachineIs(t *testing.T) {
	for _, answersIn := range []time.Duration{
		30 * time.Millisecond,  // faster than the old margin
		60 * time.Millisecond,  // slower than it: the case that failed
		120 * time.Millisecond, // much slower
	} {
		t.Run(answersIn.String(), func(t *testing.T) {
			m := newMachine(map[string]any{"ns=2;s=Loaded": false})
			// The plan is a queue, so it needs one slow answer per poll the
			// wait could make. The flag never comes up: the wait running
			// out is the point.
			slow := make([]*reaction, 0, 64)
			for i := 0; i < 64; i++ {
				slow = append(slow, &reaction{value: false, delay: answersIn, ignoreCtx: true})
			}
			m.plan("ns=2;s=Loaded", slow...)

			// A COMPUTED timeout. The clamp exists for exactly this: the
			// literal-wait scanner refuses a literal wait longer than the
			// budget at config time, so a literal can never reach the clamp.
			cfg := uatConfig(`
				opcua.wait_until({ nodeId: "ns=2;s=Loaded" }, v => v === true, msg.timeout, { pollMs: 50 });
				return opcua.success({});
			`)
			cfg.MaxScriptRuntime = 500 * time.Millisecond

			start := time.Now()
			res, _ := runScenario(t, cfg, m, map[string]any{"timeout": 5000})
			took := time.Since(start)

			// The wait ran out; that is transient, and the script's own
			// clock -- not the run's deadline -- is what ended it.
			expectOutcome(t, res, Transient)
			if took >= cfg.MaxScriptRuntime {
				t.Errorf("the run took %v and the budget is %v: it blocked to the deadline "+
					"instead of returning at the clamped timeout", took.Round(time.Millisecond),
					cfg.MaxScriptRuntime)
			}
		})
	}
}
