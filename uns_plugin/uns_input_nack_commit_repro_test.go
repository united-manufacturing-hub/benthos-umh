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

// Tests for ENG-5094: "UNS input can commit past NACKed Kafka messages".
//
// Unlike uns_input_test.go, which mocks the consumer with a boolean
// (IsCommitRecordsCalled) and therefore has NO notion of Kafka offsets, these
// tests run the REAL franz-go client against an in-memory franz-go broker
// (kfake). That means the actual commit machinery
// (DisableAutoCommit + CommitUncommittedOffsets) is exercised end to end, and
// we can read back the committed offset the broker stored.
//
// TestUNSInput_DoesNotCommitPastNACK asserts the fix and is skipped until the
// legacy uns input is fixed. TestOfficialGaplessDiscipline_NoDataLoss demonstrates the official Benthos
// kafka_franz discipline (AutoCommitMarks + Jeffail checkpointer) on the exact
// same scenario, as a no-regression reference for the eventual fix.
package uns_plugin

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

const reproTopic = "umh.messages"

// testingT is the minimal failure surface the kfake helpers need, satisfied by
// both *testing.T (this file's plain tests) and ginkgo.GinkgoT() (the uns_beta
// Ginkgo specs that share these helpers).
type testingT interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
	Cleanup(func())
}

// startBroker spins up an in-memory franz-go cluster with reproTopic
// pre-created as a single-partition topic, and returns its bootstrap address.
func startBroker(t testingT) string {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, reproTopic))
	if err != nil {
		t.Fatalf("failed to start kfake cluster: %v", err)
	}
	t.Cleanup(cluster.Close)
	addrs := cluster.ListenAddrs()
	if len(addrs) == 0 {
		t.Fatal("kfake cluster returned no listen addresses")
	}
	return addrs[0]
}

// produce writes records to reproTopic (partition 0) synchronously, so offsets
// are assigned in call order: the first record produced is offset 0, etc.
func produce(t testingT, addr string, records ...*kgo.Record) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, r := range records {
		r.Topic = reproTopic
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
}

// committedOffset reads the offset committed for the given consumer group on
// reproTopic/partition 0. The returned bool is false when the group has no
// committed offset at all.
func committedOffset(t testingT, addr string, group string) (int64, bool) {
	t.Helper()
	off, ok, err := committedOffsetE(addr, group)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return off, ok
}

// committedOffsetE is the error-returning variant of committedOffset, for
// callers off the test goroutine where Fatalf is illegal (e.g. assertions
// inside a stream consumer callback).
func committedOffsetE(addr string, group string) (int64, bool, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		return 0, false, fmt.Errorf("admin client: %w", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resps, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return 0, false, fmt.Errorf("fetch offsets: %w", err)
	}
	o, ok := resps.Lookup(reproTopic, 0)
	if !ok || o.At < 0 {
		return 0, false, nil
	}
	return o.At, true, nil
}

func rec(key string, val string) *kgo.Record {
	return &kgo.Record{Key: []byte(key), Value: []byte(val)}
}

// TestUNSInput_DoesNotCommitPastNACK covers ENG-5094:
//   - batch A (offsets 0-2) is read, then batch B (offsets 3-5) is read
//   - batch A fails at the output and is NACKed (ack called with an error)
//   - batch B succeeds (ack called with nil)
//
// The committed offset must not pass A, so a restart redelivers A. Today the
// legacy uns input commits the whole polled head (offset 6) on B's ack and
// loses A, so the test is skipped until ENG-5094 is fixed.
func TestUNSInput_DoesNotCommitPastNACK(t *testing.T) {
	t.Skip("ENG-5094: the legacy uns input commits past a NACKed batch; remove this skip with the fix")

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

// TestOfficialGaplessDiscipline_NoDataLoss demonstrates the official Benthos
// kafka_franz / redpanda input discipline on the SAME scenario, as a
// no-regression reference for the fix. It uses the exact primitives the
// official reader uses (franz_reader_unordered.go):
//   - kgo.AutoCommitMarks(): franz-go commits only *marked* records
//   - Jeffail checkpoint.Uncapped: yields only the highest gaplessly-resolved
//     record, so an unresolved (NACKed) earlier offset blocks later commits
//   - cl.MarkCommitRecords on resolve
//
// Result: while A is unresolved, NOTHING past A is committed. A is redelivered.
// Once A resolves, the checkpoint advances to B.
func TestOfficialGaplessDiscipline_NoDataLoss(t *testing.T) {
	addr := startBroker(t)
	const group = "eng5094-official"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(reproTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitMarks(), // official discipline: only marked records commit
	)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer cl.Close()

	// One checkpointer per partition (the official code keys it by partition);
	// this test uses a single partition so one suffices.
	checkpointer := checkpoint.NewUncapped[*kgo.Record]()

	// Produce + poll batch A (offsets 0-2), track it as one batch.
	produce(t, addr, rec("umh.v1.acme.a0", `{"v":0}`), rec("umh.v1.acme.a1", `{"v":1}`), rec("umh.v1.acme.a2", `{"v":2}`))
	recsA := pollRecords(ctx, t, cl, 3)
	releaseA := checkpointer.Track(recsA[len(recsA)-1], int64(len(recsA)))

	// Produce + poll batch B (offsets 3-5), track it as one batch.
	produce(t, addr, rec("umh.v1.acme.b3", `{"v":3}`), rec("umh.v1.acme.b4", `{"v":4}`), rec("umh.v1.acme.b5", `{"v":5}`))
	recsB := pollRecords(ctx, t, cl, 3)
	releaseB := checkpointer.Track(recsB[len(recsB)-1], int64(len(recsB)))

	// B succeeds first (A is still in flight / NACKed). Resolving B does NOT
	// advance the checkpoint, because A (the head) is unresolved.
	if highest := releaseB(); highest != nil {
		cl.MarkCommitRecords(*highest)
		t.Fatalf("gapless violation: resolving B while A is unresolved yielded a commit at offset %d", (*highest).Offset+1)
	}
	if err := cl.CommitMarkedOffsets(ctx); err != nil {
		t.Fatalf("commit marked: %v", err)
	}

	// THE GUARANTEE: nothing is committed while A is unresolved.
	if off, ok := committedOffset(t, addr, group); ok {
		t.Fatalf("official discipline must not commit past a NACK, but offset %d is committed", off)
	}
	t.Log("official discipline: B succeeded but A unresolved -> nothing committed, A still redeliverable")

	// And A is genuinely redeliverable: a fresh consumer in the same group
	// resumes at the start (no commit) and sees all 6 records, A included.
	if got := drainFromStart(t, addr, group+"-restart", 6); got != 6 {
		t.Fatalf("expected A redeliverable (6 records from a fresh consumer), saw %d", got)
	}

	// Now A resolves. The checkpoint advances to the highest gapless record (B's
	// last, offset 5), so the commit moves to 6 — only after the gap is filled.
	highest := releaseA()
	if highest == nil {
		t.Fatal("resolving A should advance the checkpoint to the highest gapless record")
	}
	cl.MarkCommitRecords(*highest)
	if err := cl.CommitMarkedOffsets(ctx); err != nil {
		t.Fatalf("commit marked after A resolves: %v", err)
	}
	off, ok := committedOffset(t, addr, group)
	if !ok || off != 6 {
		t.Fatalf("after A resolves, expected committed offset 6, got %d (ok=%v)", off, ok)
	}
	t.Logf("official discipline: after A resolves, checkpoint advances to %d -> at-least-once preserved", off)
}

// --- helpers ---------------------------------------------------------------

// newReproInput builds the real UnsInput (real ConsumerClient) pointed at addr.
func newReproInput(t *testing.T, addr string, group string) service.BatchInput {
	t.Helper()
	res := service.MockResources()
	cfg := UnsInputConfig{
		umhTopics:       []string{".*"},
		inputKafkaTopic: reproTopic,
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

// pollRecords polls until it has collected exactly want records.
func pollRecords(ctx context.Context, t *testing.T, cl *kgo.Client, want int) []*kgo.Record {
	t.Helper()
	var out []*kgo.Record
	for len(out) < want {
		if ctx.Err() != nil {
			t.Fatalf("timed out polling records, have %d/%d", len(out), want)
		}
		fs := cl.PollFetches(ctx)
		if err := fs.Err0(); err != nil {
			t.Fatalf("poll: %v", err)
		}
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}

// drainFromStart consumes reproTopic from the start with a brand-new group and
// returns how many records it sees (bounded wait). Used to prove records are
// still present in the log.
func drainFromStart(t *testing.T, addr string, group string, expect int) int {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(reproTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("drain client: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	count := 0
	for count < expect && ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		fs.EachRecord(func(*kgo.Record) { count++ })
	}
	return count
}

// drainGroup resumes group at its committed offset, or at the start if it
// has none, and returns how many records it reads, stopping at want.
func drainGroup(t *testing.T, addr string, group string, want int) int {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(reproTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("resume client: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	count := 0
	for count < want && ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		fs.EachRecord(func(*kgo.Record) { count++ })
	}
	return count
}
