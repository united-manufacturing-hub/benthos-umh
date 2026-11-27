// Copyright 2026 UMH Systems GmbH
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
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/handshake"
)

// These tests run the output's glue between the message and the handshake
// engine: the same script the config accepts, against a fake machine through
// the real binding.

// newHandshakeOutput builds an output whose engine runs against one fake
// machine, the way newOPCUAOutput builds it against a connection.
func newHandshakeOutput(t *testing.T, fake *fakeMachine, script string, onSuccess string, onError string) *OPCUAOutput {
	t.Helper()
	engine, err := handshake.New(handshake.Config{
		Script:              script,
		OnSuccess:           onSuccess,
		OnError:             onError,
		MaxScriptRuntime:    5 * time.Second,
		MaxOnSuccessRuntime: 5 * time.Second,
		MaxOnErrorRuntime:   5 * time.Second,
		PollInterval:        100 * time.Millisecond,
	}, &Binding{client: func() opcuaClient { return fake }})
	if err != nil {
		t.Fatalf("the engine refused a configuration the config accepted: %v", err)
	}
	return &OPCUAOutput{
		OPCUAConnection: &OPCUAConnection{Log: service.MockResources().Logger()},
		handshakeEngine: engine,
	}
}

// okMachine answers reads of Ready with true and accepts every write.
func okMachine() *fakeMachine {
	return &fakeMachine{
		readFn: func(_ context.Context, _ *ua.ReadRequest) (*ua.ReadResponse, error) {
			return &ua.ReadResponse{Results: []*ua.DataValue{
				{Status: ua.StatusOK, Value: mustVariant(ua.NewVariant(true))},
			}}, nil
		},
		writeFn: writeStatus(ua.StatusOK),
	}
}

// A delivered run hands back no error, and the machine saw the script's
// operations and the onSuccess block's, in order.
func TestRunHandshake_DeliveredReturnsNil(t *testing.T) {
	fake := okMachine()
	output := newHandshakeOutput(t, fake, `
		const READY = { nodeId: "ns=2;s=Ready" };
		if (opcua.read(READY) !== true) { return opcua.error_not_ready(); }
		opcua.write({ nodeId: "ns=2;s=Value", dataType: "Int32" }, msg.payload.value);
		return opcua.success({ value: msg.payload.value });
	`, `
		opcua.write({ nodeId: "ns=2;s=Ack", dataType: "Boolean" }, false);
	`, `
		opcua.write({ nodeId: "ns=2;s=Ack", dataType: "Boolean" }, false);
	`)
	defer func() { _ = output.Close(context.Background()) }()

	err := output.runHandshake(context.Background(), map[string]any{
		"payload": map[string]any{"value": json.Number("7")},
	})
	if err != nil {
		t.Fatalf("a delivered run is not an error: %v", err)
	}
	want := []string{
		"read ns=2;s=Ready",
		"write ns=2;s=Value",
		"write ns=2;s=Ack", // onSuccess
	}
	if strings.Join(fake.ops, "|") != strings.Join(want, "|") {
		t.Errorf("the machine saw %v, want %v", fake.ops, want)
	}
	// The value the script wrote was converted through the binding: 7 as an
	// Int32, from the message's json.Number.
	if fake.writes != 2 {
		t.Errorf("got %d writes, want 2", fake.writes)
	}
}

// A run the script ended transient is handed back as a transient error; the
// flow around the output decides what to do with it (OUT-2).
func TestRunHandshake_TransientIsHandedBack(t *testing.T) {
	fake := &fakeMachine{
		readFn: func(context.Context, *ua.ReadRequest) (*ua.ReadResponse, error) {
			return &ua.ReadResponse{Results: []*ua.DataValue{
				{Status: ua.StatusOK, Value: mustVariant(ua.NewVariant(false))},
			}}, nil
		},
		writeFn: writeStatus(ua.StatusOK),
	}
	output := newHandshakeOutput(t, fake, `
		if (opcua.read({ nodeId: "ns=2;s=Ready" }) !== true) { return opcua.error_not_ready(); }
		return opcua.success({});
	`, "", "")
	defer func() { _ = output.Close(context.Background()) }()

	err := output.runHandshake(context.Background(), map[string]any{})
	if err == nil {
		t.Fatal("a transient run must be handed back as an error")
	}
	if !strings.Contains(err.Error(), "transient") {
		t.Errorf("the error must say the run was transient: %v", err)
	}
}

// A run the script ended persistent is handed back naming the class, and a
// cleanup that did not finish makes that visible.
func TestRunHandshake_PersistentIsHandedBack(t *testing.T) {
	fake := &fakeMachine{
		readFn: readValue(ua.StatusOK, mustVariant(ua.NewVariant(true))),
		writeFn: func(_ context.Context, req *ua.WriteRequest) (*ua.WriteResponse, error) {
			if req.NodesToWrite[0].NodeID.String() == "ns=2;s=Bad" {
				return &ua.WriteResponse{Results: []ua.StatusCode{ua.StatusBadTypeMismatch}}, nil
			}
			return &ua.WriteResponse{Results: []ua.StatusCode{ua.StatusOK}}, nil
		},
	}
	output := newHandshakeOutput(t, fake, `
		return opcua.error_persistent("the machine will not take it");
	`, "", `
		opcua.write({ nodeId: "ns=2;s=Bad", dataType: "Int32" }, 5);
	`)
	defer func() { _ = output.Close(context.Background()) }()

	err := output.runHandshake(context.Background(), map[string]any{})
	if err == nil {
		t.Fatal("a persistent run must be handed back as an error")
	}
	if !strings.Contains(err.Error(), "persistent") {
		t.Errorf("the error must say the run was persistent: %v", err)
	}
	if !strings.Contains(err.Error(), "onError did not finish") {
		t.Errorf("a cleanup that did not finish must be named: %v", err)
	}
	if !strings.Contains(err.Error(), "the machine will not take it") {
		t.Errorf("the error must carry the script's detail, wrapped: %v", err)
	}
}

// A message that is not a map cannot be seen by the script.
func TestRunHandshake_RefusesAMessageThatIsNotAMap(t *testing.T) {
	output := newHandshakeOutput(t, okMachine(), `
		return opcua.success({});
	`, "", "")
	defer func() { _ = output.Close(context.Background()) }()

	err := output.runHandshake(context.Background(), []any{"not", "a", "map"})
	if err == nil {
		t.Fatal("a non-map message must be refused")
	}
	if !strings.Contains(err.Error(), "not a map") {
		t.Errorf("the error must say what the message was: %v", err)
	}
}
