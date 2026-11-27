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

// Kafka helpers shared by the uns and uns_beta tests. They run the real
// franz-go client against an in-memory franz-go broker (kfake), so tests can
// read back the offsets the broker stored.

package uns_plugin

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// testTopic is the single-partition topic every helper reads and writes.
const testTopic = "umh.messages"

// testingT is the failure surface the helpers need. *testing.T, *testing.B and
// ginkgo.GinkgoT() all satisfy it.
type testingT interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
	Cleanup(func())
}

// startBroker starts a kfake cluster with testTopic as a single-partition
// topic and returns its bootstrap address.
func startBroker(t testingT) string {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, testTopic))
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

// produce writes records to testTopic (partition 0) synchronously, so offsets
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
		r.Topic = testTopic
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
}

// rec builds a record with the given key and value.
func rec(key string, val string) *kgo.Record {
	return &kgo.Record{Key: []byte(key), Value: []byte(val)}
}

// committedOffset reads the offset committed for the given consumer group on
// testTopic/partition 0. The returned bool is false when the group has no
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
// callers where Fatalf is not allowed, such as a Gomega polling function.
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
	o, ok := resps.Lookup(testTopic, 0)
	if !ok || o.At < 0 {
		return 0, false, nil
	}
	return o.At, true, nil
}

// drainGroup resumes group at its committed offset, or at the start if it
// has none, and returns how many records it reads, stopping at want.
func drainGroup(t testingT, addr string, group string, want int) int {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(testTopic),
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
