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

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"

	"github.com/united-manufacturing-hub/benthos-umh/opcua_plugin/internal/faultopcua"
)

// TestTheControl_AnHonestServerAppliesAWrite is the control this package
// needs before any fault test can mean anything. With nothing armed, a write
// lands and the read-back returns what was written. A fault test is only
// evidence if this one is green beside it: when both go red, the server
// underneath is broken, not the code under test.
//
// It also pins the two counters a fault test reads as evidence: one dial for
// one client, no cuts, and -- the half the defect report caught -- a server
// shut down while the client is still connected must not manufacture cut
// evidence out of its own teardown.
func TestTheControl_AnHonestServerAppliesAWrite(t *testing.T) {
	srv, client, v := startServer(t, "Setpoint", int32(0))
	id := nodeID(t, v)

	if got := readValue(t, client, id); got != int32(0) {
		t.Fatalf("the node started at %v, want 0", got)
	}
	writeValue(t, client, id, int32(4200))
	if got := readValue(t, client, id); got != int32(4200) {
		t.Fatalf("read back %v, want 4200: an honest server did not apply the write", got)
	}
	// The server-side truth agrees with the client's half.
	if got := v.Value(); got != int32(4200) {
		t.Errorf("the node holds %v server-side, want 4200", got)
	}
	if n := v.Writes(); n == 0 {
		t.Errorf("the write never reached the node, so this test proved nothing")
	}
	if n := v.Reads(); n == 0 {
		t.Errorf("the read never reached the node, so this test proved nothing")
	}
	if n := srv.Dials(); n != 1 {
		t.Errorf("%d clients dialed, want 1", n)
	}
	if n := srv.Cuts(); n != 0 {
		t.Errorf("%d connections were cut on an honest machine, want 0", n)
	}

	// A shutdown is not a session drop. Closing with the client still
	// connected used to run the same cut a drop runs and land in Cuts().
	_ = srv.Close()
	if n := srv.Cuts(); n != 0 {
		t.Errorf("shutting the server down counted %d cuts, want 0: a shutdown is not a session drop", n)
	}
}

// startServer brings up a server holding one node, connects a real gopcua
// client to it, and registers the teardown for both.
func startServer(t *testing.T, node string, value any) (*faultopcua.Server, *opcua.Client, *faultopcua.Variable) {
	t.Helper()
	srv := faultopcua.New(t)
	v := srv.AddVariable(node, value)
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start server: %v", err)
	}
	client := connectClient(t, srv)
	return srv, client, v
}

// connectClient connects a real gopcua client to the server's endpoint.
func connectClient(t *testing.T, srv *faultopcua.Server) *opcua.Client {
	t.Helper()
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
	connectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Connect(connectCtx); err != nil {
		t.Fatalf("connect to %s: %v", srv.Endpoint(), err)
	}
	// Order matters, and LIFO makes it the opposite of how it reads. These
	// run last-registered-first, so the server closes BEFORE the client.
	// gopcua's server handles every request in one loop, so a fault holding
	// that loop also holds the client's CloseSession, and the client then
	// waits out its 10s default RequestTimeout. Measured: 10.56s closing the
	// client first, 0.30s closing the server first. New already registered
	// its own Close; the copy registered here runs first, and New's runs
	// again afterwards, which Close tolerates.
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	t.Cleanup(func() { _ = srv.Close() })
	return client
}

// nodeID parses the handle's identifier into the form the client's requests
// carry. The handle produces the string, so a test never assembles one by
// hand.
func nodeID(t *testing.T, v *faultopcua.Variable) *ua.NodeID {
	t.Helper()
	id, err := ua.ParseNodeID(v.ID())
	if err != nil {
		t.Fatalf("parse node id %q: %v", v.ID(), err)
	}
	return id
}

func writeValue(t *testing.T, c *opcua.Client, id *ua.NodeID, value any) ua.StatusCode {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := writeWithContext(ctx, c, id, value)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("write returned %d results, want 1", len(resp.Results))
	}
	return resp.Results[0]
}

func writeWithContext(ctx context.Context, c *opcua.Client, id *ua.NodeID, value any) (*ua.WriteResponse, error) {
	v, err := ua.NewVariant(value)
	if err != nil {
		return nil, err
	}
	return c.Write(ctx, &ua.WriteRequest{
		NodesToWrite: []*ua.WriteValue{{
			NodeID:      id,
			AttributeID: ua.AttributeIDValue,
			Value:       &ua.DataValue{EncodingMask: ua.DataValueValue, Value: v},
		}},
	})
}

func readValue(t *testing.T, c *opcua.Client, id *ua.NodeID) any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := readWithContext(ctx, c, id)
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
	resp, err := readWithContext(ctx, c, id)
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
