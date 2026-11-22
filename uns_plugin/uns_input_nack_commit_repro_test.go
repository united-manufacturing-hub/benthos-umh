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
	"errors"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// TestUNSInput_DoesNotCommitPastNACK runs the uns input with its real
// franz-go client against kfake and checks the committed offset:
//   - batch A (offsets 0-2) is read, then batch B (offsets 3-5) is read
//   - batch A fails at the output and is NACKed (ack called with an error)
//   - batch B succeeds (ack called with nil)
//
// The committed offset must not pass A, so a restart redelivers A.
// ENG-5094: the uns input commits the whole polled head (offset 6) on B's ack
// and loses A, so the test is skipped until that is fixed.
func TestUNSInput_DoesNotCommitPastNACK(t *testing.T) {
	t.Skip("ENG-5094: the uns input commits past a NACKed batch; remove this skip with the fix")

	addr := startBroker(t)
	const group = "eng5094"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	input := newReproInput(t, addr, group)
	if err := input.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = input.Close(ctx) })

	produce(t, addr, rec("umh.v1.acme.a0", `{"v":0}`), rec("umh.v1.acme.a1", `{"v":1}`), rec("umh.v1.acme.a2", `{"v":2}`))
	batchA, ackA := readNonEmpty(ctx, t, input)
	if len(batchA) != 3 {
		t.Fatalf("batch A: expected 3 messages, got %d", len(batchA))
	}

	produce(t, addr, rec("umh.v1.acme.b3", `{"v":3}`), rec("umh.v1.acme.b4", `{"v":4}`), rec("umh.v1.acme.b5", `{"v":5}`))
	batchB, ackB := readNonEmpty(ctx, t, input)
	if len(batchB) != 3 {
		t.Fatalf("batch B: expected 3 messages, got %d", len(batchB))
	}

	// A fix may replay the NACK internally and return nil, so only the
	// committed offset is asserted.
	_ = ackA(ctx, errors.New("output NACK on batch A"))
	if err := ackB(ctx, nil); err != nil {
		t.Fatalf("ackB: %v", err)
	}

	if off, ok := committedOffset(t, addr, group); ok && off > 0 {
		t.Fatalf("committed offset %d is past NACKed batch A (offsets 0-2)", off)
	}
	if err := input.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A restart of the group must redeliver A.
	if got := drainGroup(t, addr, group, 3); got < 3 {
		t.Fatalf("restart of the group saw %d records; NACKed batch A must be redelivered", got)
	}
}

// newReproInput builds the real UnsInput (real ConsumerClient) pointed at addr.
func newReproInput(t *testing.T, addr string, group string) service.BatchInput {
	t.Helper()
	res := service.MockResources()
	cfg := UnsInputConfig{
		umhTopics:       []string{".*"},
		inputKafkaTopic: testTopic,
		brokerAddress:   addr,
		consumerGroup:   group,
		metadataFormat:  defaultMetadataFormat,
	}
	in, err := NewUnsInput(NewConsumerClient(), cfg, res.Logger(), res.Metrics())
	if err != nil {
		t.Fatalf("new uns input: %v", err)
	}
	return in
}

// readNonEmpty calls ReadBatch until it returns a non-empty batch (the input
// returns (nil,nil,nil) on an empty poll, which can happen before records land).
func readNonEmpty(ctx context.Context, t *testing.T, in service.BatchInput) (service.MessageBatch, service.AckFunc) {
	t.Helper()
	for {
		batch, ackFn, err := in.ReadBatch(ctx)
		if err != nil {
			t.Fatalf("read batch: %v", err)
		}
		if len(batch) > 0 {
			return batch, ackFn
		}
		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for a non-empty batch")
		}
	}
}
