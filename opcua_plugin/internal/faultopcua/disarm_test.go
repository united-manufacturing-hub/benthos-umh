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

package faultopcua_test

import (
	"testing"

	"github.com/gopcua/opcua/ua"
)

// TestDisarm_LeavesAnHonestMachineAndKeepsTheRecord pins both halves of
// Disarm's contract: the machine goes back to answering honestly, and the
// record of the misbehavior survives, because a scenario that disarms a fault
// and then asserts on the earlier misbehavior still needs its evidence.
func TestDisarm_LeavesAnHonestMachineAndKeepsTheRecord(t *testing.T) {
	srv, client, v := startServer(t, "Flag", true)
	id := nodeID(t, v)

	f := srv.RefuseRead(v, ua.StatusBadInternalError)
	if got := readStatus(t, client, id); got != ua.StatusBadInternalError {
		t.Fatalf("the machine answered %v, want the refusal: the fault was not in effect before disarming", got)
	}
	if !f.Fired() {
		t.Fatal("the fault never fired before disarming, so this test proved nothing")
	}
	count := f.Count()

	f.Disarm()

	// An honest machine again.
	if got := readValue(t, client, id); got != true {
		t.Fatalf("after disarming, the read answered %v, want true: disarming did not restore an honest machine", got)
	}
	// And the record of the earlier misbehavior is kept.
	if !f.Fired() {
		t.Error("disarming threw the record away: a disarmed fault still has to prove it fired earlier")
	}
	if got := f.Count(); got != count {
		t.Errorf("after disarming the fault reports %d hits, want the %d it had before: the record was not kept", got, count)
	}
}

// TestDisarm_FreesTheNodeForAnotherFault pins the other half of the contract
// the second-arm guard rests on: a disarmed fault lets go of the node, so
// arming another one after it is legitimate. Disarm, then a different kind
// takes the node and both fires -- while the first fault's record survives.
func TestDisarm_FreesTheNodeForAnotherFault(t *testing.T) {
	srv, client, v := startServer(t, "Setpoint", int32(7))
	id := nodeID(t, v)

	first := srv.RefuseWrite(v, ua.StatusBadInternalError)
	if got := writeValue(t, client, id, int32(4200)); got != ua.StatusBadInternalError {
		t.Fatalf("the machine answered %v, want the refusal: the first fault was not in effect", got)
	}
	count := first.Count()

	first.Disarm()

	// The node is free, so a fault of another kind can take it, and it
	// works: the write that was refused before now lands, and the read
	// alternates.
	second := srv.FlapValue(v, int32(99))
	if got := writeValue(t, client, id, int32(4200)); got != ua.StatusOK {
		t.Fatalf("after disarming, the write answered %v, want OK: the node was not freed for the new fault", got)
	}
	if got := readValue(t, client, id); got != int32(99) {
		t.Fatalf("the new fault's first read answered %v, want 99: the second fault did not take the node", got)
	}
	if !second.Fired() {
		t.Error("the second fault never fired, so this test proved nothing")
	}

	// And the first fault's record of the earlier misbehavior is kept.
	if got := first.Count(); got != count {
		t.Errorf("after the second fault took the node, the first reports %d hits, want the %d it had: the record was not kept", got, count)
	}
}
