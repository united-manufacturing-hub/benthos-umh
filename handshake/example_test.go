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

package handshake_test

import (
	"context"
	"fmt"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/handshake"
)

// exampleMachine is one machine for the example: a binding that answers reads
// from a table and accepts writes. A real binding speaks one industrial
// protocol; this one speaks nothing. It raises its own Loaded flag on the
// second read of it, the way a machine answers a wait.
type exampleMachine struct {
	values map[string]any
	reads  map[string]int
}

func (m *exampleMachine) Namespace() string { return "opcua" }

func (m *exampleMachine) ParseAddress(raw map[string]any) (string, error) {
	id, _ := raw["nodeId"].(string)
	return id, nil
}

func (m *exampleMachine) Read(_ context.Context, addr string) (handshake.ReadResponse, error) {
	m.reads[addr]++
	if addr == "ns=3;s=Params.Loaded" && m.reads[addr] > 1 {
		return handshake.ReadResponse{Value: true}, nil
	}
	return handshake.ReadResponse{Value: m.values[addr]}, nil
}

func (m *exampleMachine) Write(_ context.Context, addr string, value any) error {
	m.values[addr] = value // the machine holds what it accepted
	return nil
}

func (m *exampleMachine) Describe(addr string) string { return addr }

// One handshake, from configuration to one delivered message: a machine whose
// apply flag must be clear before we start, a write, a wait for the machine's
// own confirmation, and the cleanup pair that returns it to idle.
func Example() {
	cfg := handshake.Config{
		Script: `
			const APPLY  = { nodeId: "ns=3;s=Params.Apply" };
			const LOADED = { nodeId: "ns=3;s=Params.Loaded" };

			if (opcua.read(APPLY) !== false || opcua.read(LOADED) !== false) {
				return opcua.error_not_ready();
			}
			opcua.write({ nodeId: "ns=3;s=Params.Value", dataType: "Int32" }, msg.payload.value);
			opcua.write(APPLY, true);
			opcua.wait_until(LOADED, v => v === true, 5000);
			return opcua.success({ value: msg.payload.value });
		`,
		OnSuccess:           `opcua.write({ nodeId: "ns=3;s=Params.Apply" }, false);`,
		OnError:             `opcua.write({ nodeId: "ns=3;s=Params.Apply" }, false);`,
		MaxScriptRuntime:    30 * time.Second,
		MaxOnSuccessRuntime: 5 * time.Second,
		MaxOnErrorRuntime:   5 * time.Second,
		PollInterval:        100 * time.Millisecond,
	}

	engine, err := handshake.New(cfg, &exampleMachine{
		values: map[string]any{"ns=3;s=Params.Apply": false, "ns=3;s=Params.Loaded": false},
		reads:  map[string]int{},
	})
	if err != nil {
		panic(err)
	}
	defer engine.Close()

	res := engine.Run(context.Background(), map[string]any{
		"payload": map[string]any{"value": 42},
	})

	switch res.Outcome {
	case handshake.Delivered:
		fmt.Println("delivered:", res.Data)
	case handshake.Transient:
		fmt.Println("hand back to the flow:", res.Err)
	case handshake.Persistent:
		fmt.Println("dead letter:", res.Err)
	}
	if res.CleanupErr != nil {
		fmt.Println("the machine may be left armed:", res.CleanupErr)
	}
	// Output: delivered: map[value:42]
}
