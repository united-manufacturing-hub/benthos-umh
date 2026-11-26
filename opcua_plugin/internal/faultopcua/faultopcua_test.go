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

// TestFault_AcceptsAWriteAndNeverAppliesIt is the misbehaviour the handshake
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
	t.Cleanup(func() { _ = srv.Close() })

	client, err := opcua.NewClient(srv.Endpoint(), opcua.SecurityMode(ua.MessageSecurityModeNone))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.Connect(connectCtx); err != nil {
		t.Fatalf("connect to %s: %v", srv.Endpoint(), err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

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
