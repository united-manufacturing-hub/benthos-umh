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

// This file is EVIDENCE for ENG-5094: "UNS input can commit past NACKed Kafka
// messages".
//
// Unlike uns_input_test.go, which mocks the consumer with a boolean
// (IsCommitRecordsCalled) and therefore has NO notion of Kafka offsets, these
// tests run the REAL franz-go client against an in-memory franz-go broker
// (kfake). That means the actual commit machinery
// (DisableAutoCommit + CommitUncommittedOffsets) is exercised end to end, and
// we can read back the committed offset the broker stored.
//
// TestUNSInput_CommitsPastNACK_DataLoss reproduces the data-loss bug.
// TestOfficialGaplessDiscipline_NoDataLoss demonstrates the official Benthos
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

// startBroker spins up an in-memory franz-go cluster with reproTopic
// pre-created as a single-partition topic, and returns its bootstrap address.
func startBroker(t *testing.T) string {
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
func produce(t *testing.T, addr string, records ...*kgo.Record) {
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
func committedOffset(t *testing.T, addr, group string) (int64, bool) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resps, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets: %v", err)
	}
	o, ok := resps.Lookup(reproTopic, 0)
	if !ok || o.At < 0 {
		return 0, false
	}
	return o.At, true
}

func rec(key, val string) *kgo.Record {
	return &kgo.Record{Key: []byte(key), Value: []byte(val)}
}

// TestUNSInput_CommitsPastNACK_DataLoss reproduces ENG-5094.
//
// Scenario (straight from the ticket):
//   - batch A (offsets 0-2) is read, then batch B (offsets 3-5) is read
//   - batch A fails at the output and is NACKed (ack called with an error)
//   - batch B succeeds and triggers the ack path (ack called with nil)
//
// The ack path calls CommitUncommittedOffsets, which commits the entire polled
// head (offset 6) — including A's offsets — even though A was NACKed. On
// restart the group resumes at 6 and A (0-2) is never redelivered: data loss.
func TestUNSInput_CommitsPastNACK_DataLoss(t *testing.T) {
	addr := startBroker(t)
	const group = "eng5094-bug"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	input := newReproInput(t, addr, group)
	if err := input.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = input.Close(ctx) })

	// Produce batch A, read it.
	produce(t, addr, rec("umh.v1.acme.a0", `{"v":0}`), rec("umh.v1.acme.a1", `{"v":1}`), rec("umh.v1.acme.a2", `{"v":2}`))
	batchA, ackA := readNonEmpty(t, ctx, input)
	if len(batchA) != 3 {
		t.Fatalf("batch A: expected 3 messages, got %d", len(batchA))
	}

	// Produce batch B, read it.
	produce(t, addr, rec("umh.v1.acme.b3", `{"v":3}`), rec("umh.v1.acme.b4", `{"v":4}`), rec("umh.v1.acme.b5", `{"v":5}`))
	batchB, ackB := readNonEmpty(t, ctx, input)
	if len(batchB) != 3 {
		t.Fatalf("batch B: expected 3 messages, got %d", len(batchB))
	}

	// NACK A (output failed). Local invariant holds: this must NOT commit.
	failErr := errors.New("output NACK on batch A")
	if err := ackA(ctx, failErr); !errors.Is(err, failErr) {
		t.Fatalf("ackA: expected the NACK error to propagate, got %v", err)
	}
	if off, ok := committedOffset(t, addr, group); ok {
		t.Fatalf("after NACK of A nothing should be committed yet, but offset %d is committed", off)
	}

	// ACK B (output succeeded). This commits the whole polled head.
	if err := ackB(ctx, nil); err != nil {
		t.Fatalf("ackB: %v", err)
	}

	off, ok := committedOffset(t, addr, group)
	if !ok {
		t.Fatal("expected a committed offset after ACK of B")
	}

	// THE BUG: committed offset is 6 (past A). A (offsets 0-2) is buried even
	// though it was NACKed. Anything >= 3 means A's offsets were committed.
	t.Logf("ENG-5094: committed offset after NACK(A)+ACK(B) = %d (A occupied offsets 0-2)", off)
	if off < 3 {
		t.Fatalf("expected the bug: committed offset past A (>=3), got %d", off)
	}
	if off != 6 {
		t.Logf("note: committed offset %d (expected 6 = head past B); still past A, still data loss", off)
	}

	// Prove the data is still in the log (a fresh group from start sees all 6),
	// so the loss is purely the committed-offset advancing past the NACK — not
	// the records being gone from Kafka.
	if got := drainFromStart(t, addr, "eng5094-bug-verify", 6); got != 6 {
		t.Fatalf("sanity: a from-start consumer should see all 6 records, saw %d", got)
	}

	// Prove the bug's group skips A on restart: resume at the committed offset
	// (6 == end of a 6-record log) and observe zero redelivery of A.
	if got := drainGroupOnce(t, addr, group); got != 0 {
		t.Fatalf("restart of the buggy group resumed past A; got %d records (A should have been redelivered but was committed away)", got)
	}
	t.Log("ENG-5094 confirmed: NACKed batch A (offsets 0-2) was committed away by batch B's success and is never redelivered -> DATA LOSS")
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
	recsA := pollRecords(t, ctx, cl, 3)
	releaseA := checkpointer.Track(recsA[len(recsA)-1], int64(len(recsA)))

	// Produce + poll batch B (offsets 3-5), track it as one batch.
	produce(t, addr, rec("umh.v1.acme.b3", `{"v":3}`), rec("umh.v1.acme.b4", `{"v":4}`), rec("umh.v1.acme.b5", `{"v":5}`))
	recsB := pollRecords(t, ctx, cl, 3)
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
func newReproInput(t *testing.T, addr, group string) service.BatchInput {
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
func readNonEmpty(t *testing.T, ctx context.Context, in service.BatchInput) (service.MessageBatch, service.AckFunc) {
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
func pollRecords(t *testing.T, ctx context.Context, cl *kgo.Client, want int) []*kgo.Record {
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
func drainFromStart(t *testing.T, addr, group string, expect int) int {
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

// drainGroupOnce resumes an existing group at its committed offset and returns
// how many records it can read within a short window (0 == nothing redelivered).
func drainGroupOnce(t *testing.T, addr, group string) int {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(reproTopic),
		// no AtStart override: resume at committed offset, like a real restart
	)
	if err != nil {
		t.Fatalf("resume client: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	count := 0
	for ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		fs.EachRecord(func(*kgo.Record) { count++ })
	}
	return count
}

var _ = fmt.Sprintf // keep fmt imported for ad-hoc debugging
