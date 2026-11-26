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
// Three assertions, and the third is the one that makes the other two mean
// something. The client failing proves only that something went wrong; the
// firing proves the fault was reached; and Cuts proves a real connection was
// closed rather than an error handed back politely.
func TestFault_DropsTheSessionOnAWrite(t *testing.T) {
	srv, client, nodeID := startServer(t, "Setpoint", int32(7))
	srv.Inject(faultopcua.Fault{Node: "Setpoint", Kind: faultopcua.DropSession})

	v, err := ua.NewVariant(int32(4200))
	if err != nil {
		t.Fatalf("variant: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Write(ctx, &ua.WriteRequest{
		NodesToWrite: []*ua.WriteValue{{
			NodeID:      nodeID,
			AttributeID: ua.AttributeIDValue,
			Value:       &ua.DataValue{EncodingMask: ua.DataValueValue, Value: v},
		}},
	})

	switch {
	case err != nil:
		// the usual shape: the socket went away mid-request
	case len(resp.Results) == 1 && resp.Results[0] != ua.StatusOK:
		// the answer raced the close out of the door; still a failure
	default:
		t.Errorf("the write reported success from a machine whose session was cut: %+v", resp)
	}

	if !srv.Fired(faultopcua.DropSession, "Setpoint") {
		t.Errorf("the fault never fired, so this proved nothing; firings: %v", srv.Firings())
	}
	if n := srv.Cuts(); n == 0 {
		t.Error("no connection was actually closed, so the client failed for some other reason")
	}

	// And the socket is really gone. Everything above is also true of a
	// server that merely answered rudely, which a mutation proved: counting
	// a cut that never closed anything killed no test. Only an operation
	// that cannot complete afterwards separates the two, so this reads a
	// node with nothing armed against it over the same client.
	srv.AddNode("Untouched", int32(1))
	other, err := ua.ParseNodeID(srv.NodeID("Untouched"))
	if err != nil {
		t.Fatalf("parse node id: %v", err)
	}
	after, cancelAfter := context.WithTimeout(context.Background(), time.Second)
	defer cancelAfter()
	if _, err := readWithContext(client, after, other); err == nil {
		t.Error("a read came back over the connection the drop was supposed to close")
	}
}
