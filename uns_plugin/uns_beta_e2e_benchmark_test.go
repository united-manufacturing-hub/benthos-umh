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
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// These benchmarks drain a corpus of records through a benthos stream (input
// -> consumer func) with the uns input and with the uns_beta input, against a
// real redpanda container. Two scenarios read the same corpus and differ only
// in umh_topics:
//
//   - match_all ('.*'): nothing is dropped. This measures the cost of
//     uns_beta's wrapper around connect's redpanda input against uns.
//   - select_one: one record of the corpus matches. Both inputs drop the
//     other records before building a message: uns filters the raw key, and
//     uns_beta's records are dropped by connect's key_pattern pre-filter.
//
// Throughput is records consumed per second; every scenario consumes the whole
// corpus. The corpus must be large enough that draining outweighs the
// consumer-group rebalance, which takes seconds. Set it with UNS_BENCH_CORPUS:
//
//	make patch-connect
//	UNS_BENCH_CORPUS=500000 go test -modfile=go.patched.mod -tags connect_patched -run=^$ -bench=BenchmarkE2E_DrainRedpanda -benchtime=3x ./uns_plugin/
//
// The rebalance allocates little, so allocs/op and B/op are comparable at any
// corpus size.

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

// benchCorpus builds n records with drop.* keys, except the last, which has the
// keep.* key select_one matches. A select_one drain therefore ends only after
// the whole corpus is consumed.
func benchCorpus(n int) []*kgo.Record {
	recs := make([]*kgo.Record, n)
	for i := range recs {
		key := fmt.Sprintf("umh.v1.drop.line.tag%d", i)
		recs[i] = &kgo.Record{Key: []byte(key), Value: []byte(`{"v":1}`)}
	}
	recs[n-1] = &kgo.Record{Key: []byte(benchTerminalKey(n)), Value: []byte(`{"v":1}`)}
	return recs
}

// benchTerminalKey is the key of the last record of an n-record corpus, the
// only one select_one matches.
func benchTerminalKey(n int) string {
	return fmt.Sprintf("umh.v1.keep.line.tag%d", n-1)
}

// benchProduce writes the whole corpus to testTopic in chunks (one ProduceSync
// per chunk keeps a large corpus from outrunning the client's record buffer).
func benchProduce(b testing.TB, addr string, recs []*kgo.Record) {
	b.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		b.Fatalf("producer client: %v", err)
	}
	defer cl.Close()
	for _, r := range recs {
		r.Topic = testTopic
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

// ensureTopic creates testTopic with one partition if it does not exist.
func ensureTopic(b testing.TB, addr string) {
	b.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		b.Fatalf("admin client: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := kadm.NewClient(cl).CreateTopic(ctx, 1, 1, nil, testTopic)
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		b.Fatalf("create topic %q: %v", testTopic, err)
	}
	if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		b.Fatalf("create topic %q: %v", testTopic, resp.Err)
	}
}

func benchInputYAML(plugin string, addr string, group string, topics string) string {
	return fmt.Sprintf(`
%s:
  broker_address: %q
  consumer_group: %q
  kafka_topic: %q
  umh_topics: [%s]
`, plugin, addr, group, testTopic, topics)
}

// BenchmarkE2E_DrainRedpanda compares uns and uns_beta against a real redpanda
// broker. It needs Docker and skips without it.
func BenchmarkE2E_DrainRedpanda(b *testing.B) {
	addr := startRedpandaBroker(b)
	ensureTopic(b, addr)
	runDrainMatrix(b, addr)
}

// runDrainMatrix produces the corpus once, then benchmarks {uns, uns_beta}
// x {match_all, select_one} draining it from addr.
func runDrainMatrix(b *testing.B, addr string) {
	benchProduce(b, addr, benchCorpus(benchCorpusSize))

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
	}

	for _, sc := range scenarios {
		for _, pl := range plugins {
			b.Run(sc.name+"/"+pl.name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					group := nextBenchGroup(pl.prefix)
					drainOnce(b, benchInputYAML(pl.name, addr, group, sc.topics), benchCorpusSize, sc.expectedDelivered)
				}
				b.StopTimer()
				b.ReportMetric(float64(benchCorpusSize*b.N)/b.Elapsed().Seconds(), "consumed/s")
			})
		}
	}
}

// startRedpandaBroker starts a redpanda container and returns its Kafka seed
// broker address. It skips the test or benchmark when Docker is unavailable,
// and fails it when Docker runs but the container does not start.
func startRedpandaBroker(b testing.TB) string {
	b.Helper()
	skipWithoutDocker(b)
	ctx := context.Background()
	container, err := redpanda.Run(ctx, "redpandadata/redpanda:latest")
	if err != nil {
		b.Fatalf("start redpanda container: %v", err)
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

// skipWithoutDocker skips when no healthy Docker provider is reachable. It is
// testcontainers.SkipIfProviderIsNotHealthy for testing.TB.
func skipWithoutDocker(b testing.TB) {
	b.Helper()
	defer func() {
		// GetProvider panics on some hosts without Docker.
		if r := recover(); r != nil {
			b.Skipf("redpanda container needs Docker: %v", r)
		}
	}()
	provider, err := testcontainers.ProviderDefault.GetProvider()
	if err != nil {
		b.Skipf("redpanda container needs Docker: %v", err)
	}
	defer func() { _ = provider.Close() }()
	if err := provider.Health(context.Background()); err != nil {
		b.Skipf("redpanda container needs Docker: %v", err)
	}
}

// drainOnce runs a fresh stream for the given input YAML over a corpus of
// corpusSize records and blocks until the corpus's terminal record arrives, by
// which point the corpus is fully consumed. It then stops the stream and fails
// unless exactly `want` records were delivered: a delivered drop record must
// not count towards a select_one drain.
func drainOnce(b testing.TB, inputYAML string, corpusSize int, want int) {
	b.Helper()
	terminalKey := benchTerminalKey(corpusSize)
	var delivered atomic.Int64
	done := make(chan struct{})
	var once sync.Once
	stop := runUnsBetaStream(b, inputYAML, func(_ context.Context, batch service.MessageBatch) error {
		delivered.Add(int64(len(batch)))
		for _, m := range batch {
			if key, _ := m.MetaGet("umh_topic"); key == terminalKey {
				once.Do(func() { close(done) })
			}
		}
		return nil
	})
	select {
	case <-done:
		stop()
	case <-time.After(300 * time.Second):
		stop()
		b.Fatalf("drain timed out before the terminal record: delivered %d/%d", delivered.Load(), want)
	}
	if got := delivered.Load(); got != int64(want) {
		b.Fatalf("drain delivered %d records, want %d", got, want)
	}
}
