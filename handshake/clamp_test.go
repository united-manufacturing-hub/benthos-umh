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
// is, as long as the binding honors the context it is given.
//
// The engine bounds each read at the wait's own end, so a machine that
// honors that bound cannot carry the run past its deadline no matter how
// slow it is. A binding that ignores its context defeats this and every
// other deadline the engine sets; nothing here can fix a binding that will
// not stop.
//
// A read is only discovered to be too late once it returns, so the clamp's
// 50ms margin alone is not enough: against a machine answering in 60ms, the
// last read starts inside the margin and runs past the deadline, and the run
// would come back Persistent after the whole budget instead of Transient at
// the clamped timeout. A machine that answers in about 0ms cannot show this.
func TestClampedWait_EndsAsAWaitHoweverSlowTheMachineIs(t *testing.T) {
	for _, answersIn := range []time.Duration{
		30 * time.Millisecond,  // faster than clampWait's margin
		60 * time.Millisecond,  // slower than the margin
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

// A slow first read must not starve the rest of the wait: with time left and
// the wait's own timeout not reached, the wait reads again. The shape is
// ordinary: the first read after a connect, or after a cache warms. Giving
// up early would turn a delivery into a false "the machine did not confirm".
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
