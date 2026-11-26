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
	"strings"
	"testing"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"

	"github.com/united-manufacturing-hub/benthos-umh/opcua_plugin/internal/faultopcua"
)

// TestTheControl_AnHonestServerAppliesAWrite is the control this package
// needs before any fault test can mean anything. With nothing armed, a write
// lands and the read-back returns what was written. A fault test is only
// evidence if this one is green beside it: when both go red, the server
// underneath is broken, not the code under test.
func TestTheControl_AnHonestServerAppliesAWrite(t *testing.T) {
	srv, client, nodeID := startServer(t, "Setpoint", int32(0))

	writeValue(t, client, nodeID, int32(4200))

	if got := readValue(t, client, nodeID); got != int32(4200) {
		t.Fatalf("read back %v, want 4200: an honest server did not apply the write", got)
	}
	if f := srv.Firings(); len(f) != 0 {
		t.Errorf("no fault was armed, but %d fired: %v", len(f), f)
	}
}

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
	srv, client, nodeID := startServer(t, "Setpoint", int32(7))
	srv.Inject(faultopcua.Fault{Node: "Setpoint", Kind: faultopcua.AcceptWriteNeverApply})

	// The machine says yes.
	if st := writeValue(t, client, nodeID, int32(4200)); st != ua.StatusOK {
		t.Fatalf("the write was refused with %v; this fault accepts the write and then ignores it", st)
	}

	// And did nothing.
	if got := readValue(t, client, nodeID); got != int32(7) {
		t.Errorf("read back %v, want the original 7: the write was applied after all", got)
	}

	if !srv.Fired(faultopcua.AcceptWriteNeverApply, "Setpoint") {
		t.Errorf("the fault never fired, so this test proved nothing; firings: %v", srv.Firings())
	}
}

// startServer brings up a server holding one node, connects a real gopcua
// client to it, and registers the teardown for both.
func startServer(t *testing.T, node string, value any) (*faultopcua.Server, *opcua.Client, *ua.NodeID) {
	t.Helper()
	ctx := context.Background()

	srv, err := faultopcua.New()
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.AddNode(node, value)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start server: %v", err)
	}

	// AutoReconnect off, for two reasons. A dropped session during a
	// handshake is the thing under test, and a client that quietly rebuilt
	// it would hide the fault. And gopcua's reconnect monitor races its own
	// Close -- Dial writes c.conn at client.go:617 while Close reads it at
	// :664, unguarded -- which this package's drop test reproduces under
	// -race every time. That is gopcua's defect, recorded in
	// DEFECT-client-close-race.md on 2026-11-02, and a caller cannot work
	// around it except by not starting the monitor.
	client, err := opcua.NewClient(srv.Endpoint(),
		opcua.SecurityMode(ua.MessageSecurityModeNone),
		opcua.AutoReconnect(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.Connect(connectCtx); err != nil {
		t.Fatalf("connect to %s: %v", srv.Endpoint(), err)
	}
	// Order matters, and LIFO makes it the opposite of how it reads. These
	// run last-registered-first, so the server closes BEFORE the client.
	// gopcua's server handles every request in one loop, so a fault holding
	// that loop also holds the client's CloseSession, and the client then
	// waits out its 10s default RequestTimeout. Measured: 10.56s closing the
	// client first, 0.30s closing the server first.
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	t.Cleanup(func() { _ = srv.Close() })

	id, err := ua.ParseNodeID(srv.NodeID(node))
	if err != nil {
		t.Fatalf("parse node id %q: %v", srv.NodeID(node), err)
	}
	return srv, client, id
}

func writeValue(t *testing.T, c *opcua.Client, id *ua.NodeID, value any) ua.StatusCode {
	t.Helper()
	v, err := ua.NewVariant(value)
	if err != nil {
		t.Fatalf("variant for %v: %v", value, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := c.Write(ctx, &ua.WriteRequest{
		NodesToWrite: []*ua.WriteValue{{
			NodeID:      id,
			AttributeID: ua.AttributeIDValue,
			Value:       &ua.DataValue{EncodingMask: ua.DataValueValue, Value: v},
		}},
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("write returned %d results, want 1", len(resp.Results))
	}
	return resp.Results[0]
}

func readValue(t *testing.T, c *opcua.Client, id *ua.NodeID) any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := c.Read(ctx, &ua.ReadRequest{
		NodesToRead: []*ua.ReadValueID{{NodeID: id, AttributeID: ua.AttributeIDValue}},
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("read returned %d results, want 1", len(resp.Results))
	}
	if st := resp.Results[0].Status; st != ua.StatusOK {
		t.Fatalf("read answered %v, want OK", st)
	}
	return resp.Results[0].Value.Value()
}

// readStatus reads a node and returns the status the machine answered with,
// rather than failing on a refusal the way readValue does.
func readStatus(t *testing.T, c *opcua.Client, id *ua.NodeID) ua.StatusCode {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := c.Read(ctx, &ua.ReadRequest{
		NodesToRead: []*ua.ReadValueID{{NodeID: id, AttributeID: ua.AttributeIDValue}},
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("read returned %d results, want 1", len(resp.Results))
	}
	return resp.Results[0].Status
}

// readWithContext reads under the caller's own deadline and hands back
// whatever happened, because a fault test is usually about the failure.
func readWithContext(ctx context.Context, c *opcua.Client, id *ua.NodeID) (*ua.ReadResponse, error) {
	return c.Read(ctx, &ua.ReadRequest{
		NodesToRead: []*ua.ReadValueID{{NodeID: id, AttributeID: ua.AttributeIDValue}},
	})
}

// TestInject_RefusesAGhostNode pins the loudest thing this package does.
//
// A fault matches by node name and the read side answers before the real
// namespace is consulted, so a fault armed against a name nothing added would
// otherwise produce its whole observable behavior against a node that is not
// there -- refusing, stalling, flapping and dropping exactly as instructed,
// with Fired reporting true and the scenario passing end to end. One typo
// between AddNode and Inject is all it takes.
func TestInject_RefusesAGhostNode(t *testing.T) {
	srv, err := faultopcua.New()
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.AddNode("Real", true)
	t.Cleanup(func() { _ = srv.Close() })

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("arming a fault against a node that does not exist was accepted")
		}
		if msg, _ := r.(string); !strings.Contains(msg, "Ghost") {
			t.Errorf("the panic should name the node, got %v", r)
		}
	}()
	srv.Inject(faultopcua.Fault{Node: "Ghost", Kind: faultopcua.RefuseRead, Status: ua.StatusBadInternalError})
}

// TestInject_RefusesAnUnknownKind pins the field beside the one the ghost
// check validates.
//
// A misspelled Kind arms a fault that matches nothing in the dispatch
// switch: the node answers honestly, nothing is recorded, and the scenario
// author is told "the fault never fired, so this test proved nothing" --
// true, and pointing at entirely the wrong thing.
func TestInject_RefusesAnUnknownKind(t *testing.T) {
	srv, err := faultopcua.New()
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.AddNode("Real", true)
	t.Cleanup(func() { _ = srv.Close() })

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("a fault kind that does not exist was accepted")
		}
		if msg, _ := r.(string); !strings.Contains(msg, "refus_read") {
			t.Errorf("the panic should name the kind, got %v", r)
		}
	}()
	srv.Inject(faultopcua.Fault{Node: "Real", Kind: faultopcua.Kind("refus_read")})
}
