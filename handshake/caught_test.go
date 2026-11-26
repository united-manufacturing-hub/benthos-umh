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
	"errors"
	"strings"
	"testing"
)

// A fault the script caught must not explain a run that something else
// stopped.
//
// The 2026-11-26 ruling -- a caught fault stays caught -- was implemented as
// precedence: an explicit ending outranks the recorded fault. That covered
// the runs that reach an ending and left the ones that do not. A script that
// caught a flaky read and then hit a bug of its own reported the MACHINE
// fault and was handed back for retry, so the same bug was persistent until
// an unrelated try/catch appeared near it, and the dead letter it eventually
// produced named a machine fault that had stopped nothing. Someone reading
// that would go and look at the network.
func TestCaughtFault_DoesNotExplainAScriptBug(t *testing.T) {
	for _, tc := range []struct {
		name    string
		script  string
		want    Outcome
		errSays string
	}{
		{
			name: "caught fault, then an uncaught exception",
			script: `
				try { opcua.read({ nodeId: "ns=2;s=Flaky" }); } catch (e) { }
				let u; return u.x;
			`,
			want:    Persistent,
			errSays: "Cannot read property",
		},
		{
			name: "caught fault, then no ending at all",
			script: `
				try { opcua.read({ nodeId: "ns=2;s=Flaky" }); } catch (e) { }
			`,
			want:    Persistent,
			errSays: "script ended without calling",
		},
		{
			// The other direction, and the one UAT-8b pinned: a fault the
			// script did NOT catch is exactly what stopped the run, so it
			// decides, and an unmarked one is worth retrying.
			name:    "the same fault uncaught still decides",
			script:  `opcua.read({ nodeId: "ns=2;s=Flaky" }); return opcua.success({});`,
			want:    Transient,
			errSays: "machine unreachable",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newMachine(map[string]any{"ns=2;s=Ready": true})
			m.plan("ns=2;s=Flaky", &reaction{err: errors.New("machine unreachable")})
			res, _ := runScenario(t, uatConfig(tc.script), m, map[string]any{})
			expectOutcome(t, res, tc.want)
			if res.Err == nil || !strings.Contains(res.Err.Error(), tc.errSays) {
				t.Errorf("the reported reason should name %q, got %v", tc.errSays, res.Err)
			}
		})
	}
}

// A fault recorded while the stack unwinds must not displace the fault that
// stopped the run.
//
// classify finds which recorded fault the run's error IS. Keeping only the
// latest meant a failing operation inside a `finally`, or a rethrow, took
// the place of the stopper: the identical machine fault was Transient until
// an unrelated caught write in an unwind path also failed, and then it was
// Persistent with an error that named neither the node nor the operation.
// The flow branches on that class.
func TestUnwindFault_DoesNotDisplaceTheFaultThatStopped(t *testing.T) {
	const script = `
		try {
			opcua.read({ nodeId: "ns=2;s=Flaky" });
		} finally {
			try { opcua.write({ nodeId: "ns=2;s=Other" }, 1); } catch (e) { }
		}
	`
	for _, tc := range []struct {
		name       string
		otherFails bool
	}{
		{"the write in the finally succeeds", false},
		{"the write in the finally also fails", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newMachine(map[string]any{"ns=2;s=Other": int64(0)})
			m.plan("ns=2;s=Flaky", &reaction{err: errors.New("machine unreachable")})
			if tc.otherFails {
				m.plan("ns=2;s=Other", &reaction{err: errors.New("write refused")})
			}
			res, _ := runScenario(t, uatConfig(script), m, map[string]any{})

			// The read is what stopped the run, it is unmarked, and an
			// unmarked fault is worth retrying. What happened in the
			// unwind path does not change that.
			expectOutcome(t, res, Transient)
			if res.Err == nil || !strings.Contains(res.Err.Error(), "machine unreachable") {
				t.Errorf("the reported fault should be the read that stopped the run, got %v", res.Err)
			}
			if res.Err != nil && !strings.Contains(res.Err.Error(), "ns=2;s=Flaky") {
				t.Errorf("the error should name the node, got %v", res.Err)
			}
		})
	}
}
