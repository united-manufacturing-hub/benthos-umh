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
// is (LIB-12) -- for a machine that honours the context it is given, which
// is what BND-3 requires of a binding.
//
// The engine bounds each read at the wait's own end, so a machine that
// honours that bound cannot carry the run past its deadline no matter how
// slow it is. A binding that IGNORES its context defeats this and every
// other deadline the engine sets; that is BND-3's own case and UAT-05
// covers it. Nothing here can fix a binding that will not stop.
//
// The clamp used to leave a fixed 50ms margin, and a read is only discovered
// to be too late once it returns, so against a machine answering in 60ms the
// last read started inside the margin and ran past the deadline: the run
// came back Persistent, blocking for the whole budget instead of returning
// at the clamped timeout. UAT-15 could not see it, because its machine
// answers in about 0ms.
//
// Predicting the next read from the last one was tried and was worse: one
// slow first read -- the ordinary shape after a connect -- ended a wait
// 130ms early on a machine that was ready on its second read, turning a
// delivery into a false "machine did not confirm".
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
				slow = append(slow, &reaction{value: false, delay: answersIn})
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

// A slow FIRST read must not starve the rest of the wait.
//
// Predicting the next read from the previous one ended a wait as soon as one
// read had been slow, even with time left and the wait's own timeout not
// reached. The shape is ordinary: the first read after a connect, or after a
// cache warms. Measured against the predictive form, this delivered on a
// generous budget and timed out on a tight one with the same machine and the
// same script -- a delivery turned into a false "the machine did not
// confirm".
func TestClampedWait_ASlowFirstReadDoesNotStarveTheWait(t *testing.T) {
	m := newMachine(map[string]any{"ns=2;s=Ready": false})
	// Slow once, then instant, and ready on the second look.
	m.plan("ns=2;s=Ready",
		&reaction{value: false, delay: 200 * time.Millisecond},
		&reaction{value: true},
		&reaction{value: true},
	)
	cfg := uatConfig(`
		opcua.wait_until({ nodeId: "ns=2;s=Ready" }, v => v === true, msg.timeout, { pollMs: 20 });
		return opcua.success({});
	`)
	cfg.MaxScriptRuntime = 400 * time.Millisecond

	res, _ := runScenario(t, cfg, m, map[string]any{"timeout": 350})
	expectOutcome(t, res, Delivered)
	if n := m.count("read", "ns=2;s=Ready"); n < 2 {
		t.Errorf("the wait gave up after %d read(s); the machine was ready on the second", n)
	}
}
