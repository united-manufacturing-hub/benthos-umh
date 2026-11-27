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

// TestFault_AcceptsAWriteAndNeverAppliesIt is the misbehavior the handshake
// reads a value back to catch: the machine answers Good and keeps its old
// value. TRUMPF's press does exactly this.
//
// The assertion comes in two halves on purpose. The client's half -- the
// read-back is stale -- cannot tell a fault that fired from a fault that
// never injected, because a server that quietly ignored the whole fault
// table would still have to be caught by something. The server's half,
// Fired, is that something.
func TestFault_AcceptsAWriteAndNeverAppliesIt(t *testing.T) {
	t.Run("control: the honest server applies the write", func(t *testing.T) {
		_, client, v := startServer(t, "Setpoint", int32(7))
		id := nodeID(t, v)

		if st := writeValue(t, client, id, int32(4200)); st != ua.StatusOK {
			t.Fatalf("the honest server refused the write with %v", st)
		}
		if got := readValue(t, client, id); got != int32(4200) {
			t.Errorf("read back %v, want 4200: an honest server did not apply the write", got)
		}
		if got := v.Value(); got != int32(4200) {
			t.Errorf("the node holds %v server-side, want 4200", got)
		}
	})

	t.Run("the machine says yes and does nothing", func(t *testing.T) {
		srv, client, v := startServer(t, "Setpoint", int32(7))
		f := srv.AcceptWriteNeverApply(v)
		id := nodeID(t, v)

		// The machine says yes.
		if st := writeValue(t, client, id, int32(4200)); st != ua.StatusOK {
			t.Fatalf("the write was refused with %v; this fault accepts the write and then ignores it", st)
		}

		// And did nothing, on the wire and in the machine alike.
		if got := readValue(t, client, id); got != int32(7) {
			t.Errorf("read back %v, want the original 7: the write was applied after all", got)
		}
		if got := v.Value(); got != int32(7) {
			t.Errorf("the node holds %v server-side, want the original 7: the write was applied after all", got)
		}

		if n := v.Writes(); n == 0 {
			t.Errorf("the write never reached the node, so this test proved nothing")
		}
		if !f.Fired() {
			t.Errorf("the fault never fired, so this test proved nothing; it took effect %d times", f.Count())
		}
	})
}

// TestFault_RefusesAWriteWithTheStatusItWasGiven is the fault a plant
// produces most and a simulator never produces: a value the machine will not
// take. The handshake reads the status off the wire and parks the value, so
// the instrument has to be able to produce any status rather than one
// stand-in refusal.
//
// Until the rewrite this fault had no test at all, which is how it shipped
// answering Good when the status was left at its zero value -- the package's
// own Fired machinery corroborating an honest answer. The arm method now
// refuses a success status; TestArming_RefusesWhatItCannotArm pins that.
func TestFault_RefusesAWriteWithTheStatusItWasGiven(t *testing.T) {
	t.Run("control: the honest server accepts the write", func(t *testing.T) {
		_, client, v := startServer(t, "Setpoint", int32(7))
		id := nodeID(t, v)

		if st := writeValue(t, client, id, int32(4200)); st != ua.StatusOK {
			t.Fatalf("the honest server refused the write with %v", st)
		}
		if got := readValue(t, client, id); got != int32(4200) {
			t.Errorf("read back %v, want 4200: an honest server did not apply the write", got)
		}
	})

	t.Run("the machine refuses with the status it was given", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			status ua.StatusCode
		}{
			{"the machine may take it next time", ua.StatusBadCommunicationError},
			{"it never will", ua.StatusBadNotWritable},
		} {
			status := tc.status
			t.Run(tc.name, func(t *testing.T) {
				srv, client, v := startServer(t, "Setpoint", int32(7))
				f := srv.RefuseWrite(v, status)
				id := nodeID(t, v)

				// The refusal, with the status the scenario asked for and
				// not some stand-in.
				if st := writeValue(t, client, id, int32(4200)); st != status {
					t.Fatalf("the write was answered with %v, want %v", st, status)
				}

				// And nothing changed, in the machine and on the wire.
				if got := v.Value(); got != int32(7) {
					t.Errorf("the node holds %v server-side, want the original 7: a refused write was applied after all", got)
				}
				if got := readValue(t, client, id); got != int32(7) {
					t.Errorf("read back %v, want the original 7: a refused write was applied after all", got)
				}

				if n := v.Writes(); n == 0 {
					t.Errorf("the write never reached the node, so this test proved nothing")
				}
				if !f.Fired() {
					t.Errorf("the fault never fired, so this test proved nothing; it took effect %d times", f.Count())
				}
			})
		}
	})
}
