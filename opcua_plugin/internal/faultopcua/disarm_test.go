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
