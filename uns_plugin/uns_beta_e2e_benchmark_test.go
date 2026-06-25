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
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// This file compares the OLD `uns` input against the NEW `uns_beta` input end to
// end: each case drains a firehose of records through a real benthos stream
// (input -> consumer func), exercising the whole framework path (Connect, fetch,
// decode, metadata, the umh_topics filter, ack/commit) rather than the regex
// filter in isolation. Two filter scenarios run against the SAME corpus,
// selected only by umh_topics:
//
//   - match_all ('.*'): nothing is dropped. Isolates the wrapping overhead of
//     delegating to the official redpanda input (uns_beta) versus the bespoke
//     consumer (uns).
//   - select_one: keep exactly ONE topic out of the whole firehose (the
//     production-realistic "subscribe to one tag" case, and the worst case for
//     build-then-filter). The OLD uns filters on the raw key BEFORE building a
//     message, so it builds ~one message; uns_beta delegates to the official
//     input, which builds a message for EVERY record, then drops all but one.
//
// Backends:
//
//   - BenchmarkE2E_DrainRedpanda runs against a REAL redpanda testcontainer.
//     This is the throughput comparison to trust. Needs Docker; skips cleanly
//     when Docker is unavailable.
//   - BenchmarkE2E_DrainKfake runs against an in-process kfake broker. kfake is
//     a correctness fake, not a performance broker: the official redpanda
//     input's franz-go consumer-session machinery (offset-list/epoch loads,
//     broker reconnects) thrashes against it and floods the trace log, so its
//     uns_beta numbers reflect that client-vs-fake-broker interaction, NOT real
//     throughput. Kept because it exercises both inputs E2E with no external
//     dependency; read it for behavior, not for rec/s.
//
// Throughput is reported as records CONSUMED per second: the whole corpus is
// consumed in every scenario, only delivery differs. The corpus must be large
// enough that draining dominates the one-time consumer-group rebalance floor
// (~seconds), otherwise rec/s reflects the floor, not steady state. Tune it with
// UNS_BENCH_CORPUS. Run with a fixed iteration count, e.g.:
//
//	UNS_BENCH_CORPUS=500000 go test -run=^$ -bench=BenchmarkE2E_DrainRedpanda -benchtime=3x ./uns_plugin/
//
// allocs/op and B/op scale with records (the rebalance floor is negligible in
// allocations), so the allocation ratios are trustworthy at any corpus size.

// benchCorpusSize is the firehose depth, overridable via UNS_BENCH_CORPUS.
var benchCorpusSize = benchEnvInt("UNS_BENCH_CORPUS", 200000)

func benchEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// umh_topics filter fragments, single-quoted so YAML treats the backslashes as
// literal regex characters (no double-escaping needed).
const (
	benchTopicsMatchAll  = `'.*'`
	benchTopicsSelectOne = `'^umh\.v1\.keep\..*'`
)

// benchGroupSeq hands out unique consumer-group names so every drain resumes
// from the start of the corpus (a fresh group has no committed offset and the
// inputs read from earliest), with no dependence on wall-clock time.
var benchGroupSeq atomic.Int64

func nextBenchGroup(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, benchGroupSeq.Add(1))
}

// benchCorpus builds the firehose: every record carries a drop.* key except the
// last, which carries the single keep.* key matched by select_one. Putting the
// one keep last means the select_one drain completes only once the ENTIRE
// firehose has been consumed (so it measures consume-all-deliver-one).
func benchCorpus() []*kgo.Record {
	recs := make([]*kgo.Record, benchCorpusSize)
	for i := range recs {
		key := fmt.Sprintf("umh.v1.drop.line.tag%d", i)
		recs[i] = &kgo.Record{Key: []byte(key), Value: []byte(`{"v":1}`)}
	}
	last := benchCorpusSize - 1
	recs[last] = &kgo.Record{
		Key:   []byte(fmt.Sprintf("umh.v1.keep.line.tag%d", last)),
		Value: []byte(`{"v":1}`),
	}
	return recs
}

// benchProduce writes the whole corpus to reproTopic in chunks (one ProduceSync
// per chunk keeps a large corpus from outrunning the client's record buffer).
func benchProduce(b *testing.B, addr string, recs []*kgo.Record) {
	b.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		b.Fatalf("producer client: %v", err)
	}
	defer cl.Close()
	for _, r := range recs {
		r.Topic = reproTopic
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	const chunk = 10000
	for start := 0; start < len(recs); start += chunk {
		end := min(start+chunk, len(recs))
		if err := cl.ProduceSync(ctx, recs[start:end]...).FirstErr(); err != nil {
			b.Fatalf("produce corpus: %v", err)
		}
	}
}

// ensureTopic creates reproTopic (single partition, for offset-order parity with
// kfake) if it does not already exist. Used for the redpanda backend, where the
// topic is not pre-seeded.
func ensureTopic(b *testing.B, addr string) {
	b.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		b.Fatalf("admin client: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := kadm.NewClient(cl).CreateTopic(ctx, 1, 1, nil, reproTopic)
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		b.Fatalf("create topic %q: %v", reproTopic, err)
	}
	if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		b.Fatalf("create topic %q: %v", reproTopic, resp.Err)
	}
}

func benchInputYAML(plugin string, addr string, group string, topics string) string {
	return fmt.Sprintf(`
%s:
  broker_address: %q
  consumer_group: %q
  kafka_topic: %q
  umh_topics: [%s]
`, plugin, addr, group, reproTopic, topics)
}

// BenchmarkE2E_DrainRedpanda is the throughput comparison to trust: both inputs
// run against a real redpanda broker. Needs Docker; skips otherwise.
func BenchmarkE2E_DrainRedpanda(b *testing.B) {
	addr := startRedpandaBroker(b)
	ensureTopic(b, addr)
	runDrainMatrix(b, addr)
}

// BenchmarkE2E_DrainKfake runs the same matrix against an in-process kfake
// broker. See the file header: uns_beta numbers here reflect franz-go vs the
// fake broker, not real throughput.
func BenchmarkE2E_DrainKfake(b *testing.B) {
	runDrainMatrix(b, startBroker(b))
}

// runDrainMatrix produces the corpus once, then benchmarks {uns, uns_beta,
// uns_beta_single} x {match_all, select_one} draining it from addr.
func runDrainMatrix(b *testing.B, addr string) {
	benchProduce(b, addr, benchCorpus())

	scenarios := []struct {
		name              string
		topics            string
		expectedDelivered int
	}{
		{"match_all", benchTopicsMatchAll, benchCorpusSize},
		{"select_one", benchTopicsSelectOne, 1},
	}
	plugins := []struct {
		name   string
		prefix string
	}{
		{"uns", "bench-uns"},
		{"uns_beta", "bench-unsbeta"},
		{"uns_beta_single", "bench-unsbetasingle"},
	}

	for _, sc := range scenarios {
		for _, pl := range plugins {
			b.Run(sc.name+"/"+pl.name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					group := nextBenchGroup(pl.prefix)
					drainOnce(b, benchInputYAML(pl.name, addr, group, sc.topics), sc.expectedDelivered)
				}
				b.StopTimer()
				b.ReportMetric(float64(benchCorpusSize*b.N)/b.Elapsed().Seconds(), "consumed/s")
			})
		}
	}
}

// startRedpandaBroker starts a real redpanda container and returns its Kafka
// seed-broker address. It skips the benchmark when Docker/redpanda is
// unavailable, so `-bench=.` stays runnable in environments without Docker.
func startRedpandaBroker(b *testing.B) string {
	b.Helper()
	ctx := context.Background()
	container, err := redpanda.Run(ctx, "redpandadata/redpanda:latest")
	if err != nil {
		b.Skipf("redpanda container unavailable (needs Docker): %v", err)
	}
	b.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = container.Terminate(cctx)
	})
	addr, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		b.Fatalf("kafka seed broker: %v", err)
	}
	return addr
}

// drainOnce runs a fresh stream for the given input YAML and blocks until the
// consumer has been delivered `want` records (by which point the corpus is
// fully consumed), then stops the stream.
func drainOnce(b *testing.B, inputYAML string, want int) {
	b.Helper()
	var delivered atomic.Int64
	done := make(chan struct{})
	var once sync.Once
	stop := runUnsBetaStream(b, inputYAML, func(_ context.Context, batch service.MessageBatch) error {
		if delivered.Add(int64(len(batch))) >= int64(want) {
			once.Do(func() { close(done) })
		}
		return nil
	})
	select {
	case <-done:
		stop()
	case <-time.After(300 * time.Second):
		stop()
		b.Fatalf("drain timed out: delivered %d/%d", delivered.Load(), want)
	}
}
