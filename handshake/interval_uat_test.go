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
	"strings"
	"testing"
)

// TestWaitUntil_RefusesAComputedTimeoutLongerThanAnHour pins the run-time
// half of the hour rule. A literal wait that long is already refused when
// the config loads: Validate adds up the literal waits in each block and
// rejects a total that exceeds the block's budget. What Validate cannot see
// is a timeout the script computes, such as
// wait_until(addr, pred, msg.timeout) with a value in the wrong unit.
//
// It is persistent. Repeating the message will not make the number smaller.
func TestWaitUntil_RefusesAComputedTimeoutLongerThanAnHour(t *testing.T) {
	m := newMachine(map[string]any{"ns=2;s=Ready": false})
	cfg := uatConfig(`
		opcua.wait_until({ nodeId: "ns=2;s=Ready" }, v => v === true, msg.timeout);
		return opcua.success({});
	`)
	res, _ := runScenario(t, cfg, m, map[string]any{"timeout": 3600001})
	expectOutcome(t, res, Persistent)
	if res.Err == nil || !strings.Contains(res.Err.Error(), "check the units") {
		t.Errorf("the refusal should name the units, got %v", res.Err)
	}
	if n := m.count("read", "ns=2;s=Ready"); n != 0 {
		t.Errorf("the machine was read %d time(s) for a wait that should never have started", n)
	}
}
