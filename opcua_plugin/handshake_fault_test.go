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

package opcua_plugin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/handshake"
	"github.com/united-manufacturing-hub/benthos-umh/opcua_plugin/internal/faultopcua"
)

// The handshake exists to survive a machine behaving badly, and until now
// nothing had ever made one behave badly: opc-plc answers correctly by
// design, so every scenario in the suite was a scenario the feature would
// have passed without being written. These drive the real script through the
// real engine, the real binding and a real socket into a server that
// refuses, stalls and drops (ENG-8131).
//
// Each scenario asserts twice. What the output did with the message, and
// that the fault actually fired: a fault that silently failed to inject
// would otherwise read as a fault the handshake handled.

// TestFault_AWriteTheMachineNeverApplies is the reason the read-back rule
// exists. The machine answers Good and keeps its old value, so only reading
// it back finds out, and the message must not be acknowledged.
func TestFault_AWriteTheMachineNeverApplies(t *testing.T) {
	srv := startFaultServer(t, map[string]any{"Setpoint": int32(7)})
	srv.Inject(faultopcua.Fault{Node: "Setpoint", Kind: faultopcua.AcceptWriteNeverApply})

	err := runFaultHandshake(t, srv, handshake.Config{
		Script: `
			const SETPOINT = { nodeId: "` + srv.NodeID("Setpoint") + `", dataType: "Int32" };
			opcua.write(SETPOINT, 4200);
			if (opcua.read(SETPOINT) !== 4200) {
				return opcua.error_persistent("the machine did not take the setpoint");
			}
			return opcua.success({});
		`,
		MaxScriptRuntime: 5 * time.Second,
		PollInterval:     50 * time.Millisecond,
	}, map[string]any{})

	assertOutcome(t, err, "persistent")
	assertFired(t, srv, faultopcua.AcceptWriteNeverApply, "Setpoint")
}

// TestFault_AReadTheMachineRefuses is the classification the whole outcome
// model turns on. The same shape of refusal carries both answers, and the
// status code is the only thing that separates a message worth retrying
// from one that must be parked.
func TestFault_AReadTheMachineRefuses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status ua.StatusCode
		want   string
	}{
		{"a machine that may answer next time", ua.StatusBadCommunicationError, "transient"},
		{"a node that does not exist", ua.StatusBadNodeIDUnknown, "persistent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := startFaultServer(t, map[string]any{"Ready": true})
			srv.Inject(faultopcua.Fault{Node: "Ready", Kind: faultopcua.RefuseRead, Status: tc.status})

			err := runFaultHandshake(t, srv, handshake.Config{
				Script: `
					opcua.read({ nodeId: "` + srv.NodeID("Ready") + `" });
					return opcua.success({});
				`,
				MaxScriptRuntime: 5 * time.Second,
				PollInterval:     50 * time.Millisecond,
			}, map[string]any{})

			assertOutcome(t, err, tc.want)
			// The error has to name the status the machine answered with.
			// Without this, a binding that ignored the status entirely
			// still failed the run -- the empty value would not convert --
			// and the scenario passed for a reason that has nothing to do
			// with what it is testing. A mutation proved exactly that.
			if err != nil && !strings.Contains(err.Error(), tc.status.Error()) {
				t.Errorf("the failure should name %v, got %v", tc.status, err)
			}
			assertFired(t, srv, faultopcua.RefuseRead, "Ready")
		})
	}
}

// TestFault_AMachineThatConfirmsLate is the ordinary case wait_until exists
// for, and the one a fixture whose flag is already up never reaches. The
// machine is working; it is just not finished yet.
func TestFault_AMachineThatConfirmsLate(t *testing.T) {
	srv := startFaultServer(t, map[string]any{"Loaded": true})
	srv.Inject(faultopcua.Fault{
		Node:  "Loaded",
		Kind:  faultopcua.RaiseFlagLate,
		After: 500 * time.Millisecond,
		Until: false,
	})

	start := time.Now()
	err := runFaultHandshake(t, srv, handshake.Config{
		Script: `
			opcua.wait_until({ nodeId: "` + srv.NodeID("Loaded") + `" }, v => v === true, 5000, { pollMs: 50 });
			return opcua.success({});
		`,
		MaxScriptRuntime: 10 * time.Second,
		PollInterval:     50 * time.Millisecond,
	}, map[string]any{})

	assertOutcome(t, err, "delivered")
	if waited := time.Since(start); waited < 400*time.Millisecond {
		t.Errorf("the run finished in %v, so it never waited for the flag", waited)
	}
	assertFired(t, srv, faultopcua.RaiseFlagLate, "Loaded")
}

// TestFault_AMachineThatAnswersTooLate is the deadline. The machine answers
// correctly throughout, so nothing is wrong with it except the clock, and
// the run has to end on its own timing rather than on the machine's.
func TestFault_AMachineThatAnswersTooLate(t *testing.T) {
	srv := startFaultServer(t, map[string]any{"Ready": true})
	srv.Inject(faultopcua.Fault{Node: "Ready", Kind: faultopcua.AnswerLate, Delay: 3 * time.Second})

	start := time.Now()
	err := runFaultHandshake(t, srv, handshake.Config{
		Script: `
			opcua.read({ nodeId: "` + srv.NodeID("Ready") + `" });
			return opcua.success({});
		`,
		MaxScriptRuntime: 700 * time.Millisecond,
		PollInterval:     50 * time.Millisecond,
	}, map[string]any{})

	assertOutcome(t, err, "persistent")
	if waited := time.Since(start); waited > 2500*time.Millisecond {
		t.Errorf("the run took %v, so it waited on the machine rather than on its own deadline", waited)
	}
	assertFired(t, srv, faultopcua.AnswerLate, "Ready")
}

// TestFault_TheSessionDropsOnTheCommittingWrite is the half-state onError
// exists for. The write reached the machine, the socket died before any
// answer, and nobody can now say whether it landed.
func TestFault_TheSessionDropsOnTheCommittingWrite(t *testing.T) {
	srv := startFaultServer(t, map[string]any{"Apply": false})
	srv.Inject(faultopcua.Fault{Node: "Apply", Kind: faultopcua.DropSession})

	err := runFaultHandshake(t, srv, handshake.Config{
		Script: `
			opcua.write({ nodeId: "` + srv.NodeID("Apply") + `", dataType: "Boolean" }, true);
			return opcua.success({});
		`,
		MaxScriptRuntime: 5 * time.Second,
		PollInterval:     50 * time.Millisecond,
	}, map[string]any{})

	if err == nil {
		t.Error("the message was acknowledged although nobody can say whether the write landed")
	}
	assertFired(t, srv, faultopcua.DropSession, "Apply")
	if n := srv.Cuts(); n == 0 {
		t.Error("no connection was closed, so the run failed for some other reason")
	}
}

// startFaultServer brings up a fault server holding the given nodes.
func startFaultServer(t *testing.T, nodes map[string]any) *faultopcua.Server {
	t.Helper()
	srv, err := faultopcua.New()
	if err != nil {
		t.Fatalf("fault server: %v", err)
	}
	for name, value := range nodes {
		srv.AddNode(name, value)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start fault server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

// runFaultHandshake drives one message down the path a deployed config
// takes: the output's own handshake entry, the engine, the binding, and a
// gopcua client on a real socket.
func runFaultHandshake(t *testing.T, srv *faultopcua.Server, cfg handshake.Config, msg map[string]any) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := &OPCUAConnection{
		Endpoint:      srv.Endpoint(),
		Insecure:      true,
		DirectConnect: true,
		Log:           service.MockResources().Logger(),
	}
	if err := conn.connect(ctx); err != nil {
		t.Fatalf("connect to the fault server at %s: %v", srv.Endpoint(), err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	engine, err := handshake.New(cfg, NewBinding(conn))
	if err != nil {
		t.Fatalf("build the handshake engine: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	out := &OPCUAOutput{OPCUAConnection: conn, handshakeEngine: engine}
	return out.runHandshake(ctx, msg)
}

// assertOutcome reads the outcome off the error the output hands the flow,
// which is the only place the class appears -- an output is a sink, so the
// class travels as a prefix on an error (MORNING.md question 4).
func assertOutcome(t *testing.T, err error, want string) {
	t.Helper()
	switch want {
	case "delivered":
		if err != nil {
			t.Errorf("the run should have delivered, got %v", err)
		}
	default:
		if err == nil {
			t.Errorf("the run should have ended %s, but the message was acknowledged", want)
			return
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the run should have ended %s, got %v", want, err)
		}
	}
}

func assertFired(t *testing.T, srv *faultopcua.Server, kind faultopcua.Kind, node string) {
	t.Helper()
	if !srv.Fired(kind, node) {
		t.Errorf("the %s fault never fired on %s, so this scenario proved nothing; firings: %v",
			kind, node, srv.Firings())
	}
}

// The scenarios below are PR1's UAT cases re-run against a real machine.
// Each keeps its UAT number, because the point is that the same situation
// now has two witnesses: the fake machine in handshake/uat_test.go, which
// answers from a plan, and a server on a socket, which answers from its own
// state. A fake that quietly disagrees with a real server is the most
// expensive kind of test there is, and the only way to find one is to run
// both.
//
// The UAT cases NOT re-run here are the ones about the engine rather than
// the machine: an uncaught exception, a script with no ending, a script that
// catches its own ending, nothing carrying between runs, a config refused at
// deploy time. A real server cannot say anything about those.

// UAT-02 and UAT-03: the machine is never ready. The wait runs out, and
// what that means is the script's to declare -- try again later by default,
// park it when the script knows the flag is never coming.
func TestFaultUAT0203_AMachineThatIsNeverReady(t *testing.T) {
	for _, tc := range []struct {
		name      string
		onTimeout string
		want      string
	}{
		{"by default the message is worth retrying", "", "transient"},
		{"the script knows better and says so", `, { onTimeout: "persistent" }`, "persistent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := startFaultServer(t, map[string]any{"Ready": true})
			// Held down for longer than the wait, so the flag never comes.
			srv.Inject(faultopcua.Fault{
				Node:  "Ready",
				Kind:  faultopcua.RaiseFlagLate,
				After: time.Hour,
				Until: false,
			})

			err := runFaultHandshake(t, srv, handshake.Config{
				Script: `
					opcua.wait_until({ nodeId: "` + srv.NodeID("Ready") + `" }, v => v === true, 300` +
					tc.onTimeout + `);
					return opcua.success({});
				`,
				MaxScriptRuntime: 5 * time.Second,
				PollInterval:     50 * time.Millisecond,
			}, map[string]any{})

			assertOutcome(t, err, tc.want)
			assertFired(t, srv, faultopcua.RaiseFlagLate, "Ready")
		})
	}
}

// UAT-06 and UAT-07: the machine refuses the committing write. Whether the
// message can be retried is the status code's to say, and this is the
// commonest persistent fault a plant produces -- a value the machine will
// not take. A simulator never produces it, because a simulator takes
// everything.
func TestFaultUAT0607_TheMachineRefusesTheCommittingWrite(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status ua.StatusCode
		want   string
	}{
		{"a value this machine will never accept", ua.StatusBadTypeMismatch, "persistent"},
		{"a machine that is briefly unreachable", ua.StatusBadCommunicationError, "transient"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := startFaultServer(t, map[string]any{"Setpoint": int32(0)})
			srv.Inject(faultopcua.Fault{Node: "Setpoint", Kind: faultopcua.RefuseWrite, Status: tc.status})

			err := runFaultHandshake(t, srv, handshake.Config{
				Script: `
					opcua.write({ nodeId: "` + srv.NodeID("Setpoint") + `", dataType: "Int32" }, 4200);
					return opcua.success({});
				`,
				MaxScriptRuntime: 5 * time.Second,
				PollInterval:     50 * time.Millisecond,
			}, map[string]any{})

			assertOutcome(t, err, tc.want)
			assertFired(t, srv, faultopcua.RefuseWrite, "Setpoint")
		})
	}
}

// UAT-19: the flag flaps. Two controllers, or a machine that raises a flag
// and drops it again, so one read says done and the next says not. A
// handshake that confirms on a single read believes the first answer it
// likes; wait_until's predicate has to hold on the read that satisfies it.
func TestFaultUAT19_AFlagThatFlaps(t *testing.T) {
	srv := startFaultServer(t, map[string]any{"Loaded": true})
	srv.Inject(faultopcua.Fault{Node: "Loaded", Kind: faultopcua.FlapValue, Until: false})

	err := runFaultHandshake(t, srv, handshake.Config{
		Script: `
			opcua.wait_until({ nodeId: "` + srv.NodeID("Loaded") + `" }, v => v === true, 3000, { pollMs: 20 });
			return opcua.success({});
		`,
		MaxScriptRuntime: 10 * time.Second,
		PollInterval:     20 * time.Millisecond,
	}, map[string]any{})

	// A flapping flag is still up half the time, so the wait is satisfied.
	// What this scenario pins is that it is satisfied by a read that saw
	// the flag up, rather than by a stale or skipped one.
	assertOutcome(t, err, "delivered")
	assertFired(t, srv, faultopcua.FlapValue, "Loaded")

	// Asserting "delivered" alone would pass against a flag that never
	// flapped at all, so count the reads: the flag is down on the first one
	// and the wait has to come back for another. One read means the
	// scenario never exercised the flapping it is named for.
	reads := 0
	for _, f := range srv.Firings() {
		if f.Kind == faultopcua.FlapValue {
			reads++
		}
	}
	if reads < 2 {
		t.Errorf("the flag was read %d time(s); the wait was satisfied without ever seeing it down", reads)
	}
}
