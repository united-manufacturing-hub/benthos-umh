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
	"context"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"

	"github.com/united-manufacturing-hub/benthos-umh/opcua_plugin/internal/faultopcua"
)

// TestFault_DropsTheSessionOnAWrite is the half-state the onError block
// exists for. The write reaches the machine and the socket dies before any
// answer comes back, so the caller cannot find out whether it landed, and
// retrying might do it twice.
//
// The evidence comes in three layers, and the third is what makes the other
// two mean something. The client failing proves only that something went
// wrong; the fault firing proves the fault was reached; and Cuts proves a
// real connection was closed rather than an error handed back politely.
func TestFault_DropsTheSessionOnAWrite(t *testing.T) {
	t.Run("control: the honest server keeps the session a write rode in on", func(t *testing.T) {
		srv, client, v := startServer(t, "Setpoint", int32(7))
		id := nodeID(t, v)

		if st := writeValue(t, client, id, int32(4200)); st != ua.StatusOK {
			t.Fatalf("the honest server refused the write with %v", st)
		}
		if got := readValue(t, client, id); got != int32(4200) {
			t.Errorf("read back %v, want 4200", got)
		}
		if n := srv.Dials(); n != 1 {
			t.Errorf("%d clients dialed, want 1", n)
		}
		if n := srv.Cuts(); n != 0 {
			t.Errorf("%d connections were cut on an honest machine, want 0", n)
		}

		// A shutdown is not a session drop either, so closing with the
		// client still connected must not count as a cut.
		_ = srv.Close()
		if n := srv.Cuts(); n != 0 {
			t.Errorf("shutting the server down counted %d cuts, want 0: a shutdown is not a session drop", n)
		}
	})

	t.Run("the socket underneath the write dies", func(t *testing.T) {
		srv, client, v := startServer(t, "Setpoint", int32(7))
		untouched := srv.AddVariable("Untouched", int32(1))
		f := srv.DropSession(v)
		id := nodeID(t, v)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := writeWithContext(ctx, client, id, int32(4200))

		switch {
		case err != nil:
			// the usual shape: the socket went away mid-request
		case len(resp.Results) == 1 && resp.Results[0] != ua.StatusOK:
			// the answer raced the close out of the door; still a failure
		default:
			t.Errorf("the write reported success from a machine whose session was cut: %+v", resp)
		}

		if !f.Fired() {
			t.Errorf("the fault never fired, so this proved nothing; it took effect %d times", f.Count())
		}
		if n := srv.Cuts(); n != 1 {
			t.Errorf("%d connections were closed by the drop, want exactly 1: Cuts is the independent witness beside Fault.Count", n)
		}

		// And the socket is really gone. Everything above is also true of a
		// server that merely answered rudely and counted a cut it never
		// made. Only an operation that cannot complete afterwards separates
		// the two, so this reads a node with nothing armed against it over
		// the same client.
		after, cancelAfter := context.WithTimeout(context.Background(), time.Second)
		defer cancelAfter()
		if _, err := readWithContext(after, client, nodeID(t, untouched)); err == nil {
			t.Error("a read came back over the connection the drop was supposed to close")
		}

		// The drop is also visible from outside the client: a second client
		// that connects afterwards is a second dial.
		fresh := connectClient(t, srv)
		if got := readValue(t, fresh, nodeID(t, untouched)); got != int32(1) {
			t.Errorf("a fresh client read %v, want 1: the drop broke the server for everyone, not just the dropped session", got)
		}
		if n := srv.Dials(); n != 2 {
			t.Errorf("%d clients dialed in total, want 2: a reconnect is supposed to be visible here", n)
		}
	})

	// The drop cuts the connection that touched the node, and only that one:
	// a second client must not lose its session to a fault it never came
	// near.
	t.Run("the client that never touched the node keeps its session", func(t *testing.T) {
		srv := faultopcua.New(t)
		doomed := srv.AddVariable("Doomed", int32(7))
		spared := srv.AddVariable("Spared", int32(1))
		if err := srv.Start(context.Background()); err != nil {
			t.Fatalf("start server: %v", err)
		}

		toucher := connectClient(t, srv) // will touch the faulted node
		bystander := connectClient(t, srv)
		f := srv.DropSession(doomed)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := writeWithContext(ctx, toucher, nodeID(t, doomed), int32(4200))
		switch {
		case err != nil:
			// the socket went away mid-request, as it should
		case len(resp.Results) == 1 && resp.Results[0] != ua.StatusOK:
			// the answer raced the close out of the door; still a failure
		default:
			t.Errorf("the write reported success from a machine whose session was cut: %+v", resp)
		}

		if !f.Fired() {
			t.Errorf("the fault never fired, so this proved nothing")
		}
		if n := srv.Cuts(); n != 1 {
			t.Errorf("%d connections were closed by the drop, want exactly 1: a bystander's session is not drop evidence", n)
		}

		// The bystander never touched the node, so its session is intact
		// and it can still work.
		if got := readValue(t, bystander, nodeID(t, spared)); got != int32(1) {
			t.Errorf("the bystander read %v over the connection the drop was supposed to leave alone, want 1", got)
		}
		if n := srv.Dials(); n != 2 {
			t.Errorf("%d clients dialed, want 2", n)
		}
	})
}
