//go:build connect_patched

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

package uns_plugin

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	// The forwarder shim is created by patches/connect-redpanda-forward.patch
	// and is present only after `make patch-connect` (hence the connect_patched
	// build tag).
	"github.com/redpanda-data/connect/v4/public/components/kafka/redpandaforward"
)

// TestRedpandaForwarderConnects asserts that
// redpandaforward.NewRedpandaInput(conf, res) returns a non-nil
// service.BatchInput that successfully Connects to a real in-process kfake
// broker. This R0b rung verifies construction + Connect only; consumption
// parity (ReadBatch yielding records, NACK replay) is deferred to R1b (delivery
// end-to-end), so the test does not yet produce records or assert a batch.
// test does not yet produce records or assert a batch.
func TestRedpandaForwarderConnects(t *testing.T) {
	// Real in-process kfake broker (the same helper the uns_beta integration
	// suite uses), pre-creating the umh.messages single-partition topic.
	addr := startBroker(t)

	// Minimal redpanda input config: the pre-created topic, a unique consumer
	// group, and auto_replay_nacks pinned true.
	const group = "r0b-forwarder-construct"
	yaml := fmt.Sprintf(`
seed_brokers:
  - %s
topics:
  - umh.messages
consumer_group: %s
auto_replay_nacks: true
`, addr, group)

	spec := service.NewConfigSpec().Fields(redpandaforward.ConfigFields()...)
	conf, err := spec.ParseYAML(yaml, nil)
	if err != nil {
		t.Fatalf("ParseYAML against forwarder ConfigFields() failed; the shim must re-export a schema that accepts a minimal explicit-connection redpanda config: %v", err)
	}

	// The outer stream's *service.Resources; MockResources provides the logger
	// and metrics registry the reader's poll goroutine needs (it creates a
	// redpanda_lag gauge).
	res := service.MockResources()

	inner, err := redpandaforward.NewRedpandaInput(conf, res)
	if err != nil {
		t.Fatalf("NewRedpandaInput returned an error; a stub returning (nil, ErrNotImplemented) fails this test, and the real constructor must succeed against a live broker: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = inner.Close(ctx)
	})

	// Connect must succeed against the live broker within a bounded timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := inner.Connect(ctx); err != nil {
		t.Fatalf("inner.Connect to the kfake broker returned an error; the forwarder must construct a redpanda reader that dials and Connects against a live broker: %v", err)
	}
}
