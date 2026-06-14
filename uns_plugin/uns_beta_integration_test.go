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

// The startBroker/produce/rec/committedOffsetE helpers used here live in
// uns_input_nack_commit_repro_test.go (same package).

package uns_plugin

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
	// Registers benthos core components (incl. the "none" tracer that
	// StreamBuilder.Build defaults to). The production binary gets these via
	// its full component bundle; this minimal test package only blank-imports
	// the kafka components, so the StreamBuilder build needs pure here.
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/twmb/franz-go/pkg/kgo"
)

// runUnsBetaStream starts a StreamBuilder pipeline: uns_beta input -> consumerFn.
// consumerFn's returned error NACKs the batch. Returns a stop func.
func runUnsBetaStream(t testingT, unsBetaYAML string, consumerFn func(context.Context, service.MessageBatch) error) (stop func()) {
	t.Helper()
	sb := service.NewStreamBuilder()
	if err := sb.AddInputYAML(unsBetaYAML); err != nil {
		t.Fatalf("input yaml: %v", err)
	}
	if err := sb.AddBatchConsumerFunc(consumerFn); err != nil {
		t.Fatalf("consumer: %v", err)
	}
	stream, err := sb.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error // written before close(done), read after <-done
	go func() { defer close(done); runErr = stream.Run(ctx) }()
	return func() {
		cancel()
		<-done
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			t.Fatalf("stream run: %v", runErr)
		}
	}
}

var _ = Describe("uns_beta input delivery", Label("uns_beta"), func() {
	It("delivers a produced message and commits its offset on ack", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-delivery"
		produce(GinkgoT(), addr, rec("umh.v1.acme.berlin.temp", `{"v":1}`))

		var mu sync.Mutex
		var got []string
		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
`, func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range b {
				bs, _ := m.AsBytes()
				got = append(got, string(bs))
			}
			return nil
		})
		defer stop()

		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return len(got)
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(BeNumerically(">=", 1), "message never arrived through uns_beta")
		mu.Lock()
		first := got[0]
		mu.Unlock()
		Expect(first).To(Equal(`{"v":1}`))

		// Stop the stream, then verify the ack actually committed the offset. A
		// mis-wired ack path (or a consumer_group dropped from the redpanda
		// config built by newUnsBetaReader) would deliver fine here but
		// replay the full topic on every restart in production. The inner
		// redpanda input commits on the 5s commit_period tick pinned in
		// the uns_beta template (uns_beta_template.yaml), so Gomega's 1s default
		// timeout would flake.
		stop()
		Eventually(func(g Gomega) {
			off, ok, err := committedOffsetE(addr, group)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "no offset committed yet")
			g.Expect(off).To(Equal(int64(1)))
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	})
})

// Every emitted message carries umh_topic and kafka_msg_key as aliases of the
// kafka_key metadata, alongside the metadata the delegated redpanda input sets
// natively (kafka_key, kafka_topic, kafka_timestamp_ms, headers). kafka_key
// normally holds the record key, so the aliases override any umh_topic header
// that passed through from the producer; a producer header literally named
// "kafka_key", however, shadows the record key before the umh_topics filter
// runs and before the aliases are stamped (the documented gap pinned below).
// For non-UMH producers that send no headers the aliases are the only source
// of umh_topic. (ENG-5094)
var _ = Describe("uns_beta metadata contract", Label("uns_beta"), func() {
	It("aliases each message's own record key and passes the native redpanda metadata through", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-meta"
		const key1 = "umh.v1.acme.k1"
		const key2 = "umh.v1.acme.k2"
		// Two records with distinct keys pin per-message aliasing: a regression
		// that stamps the first record's key onto every message in a fetch
		// batch would reroute data to the wrong UNS topic without any error.
		produce(GinkgoT(), addr,
			&kgo.Record{
				Key:   []byte(key1),
				Value: []byte(`{"v":1}`),
				// Multi-byte header values read back as plain strings.
				// Single-byte and empty header values are not asserted here.
				// The umh_topic header pins that the alias overrides a
				// divergent producer-supplied value.
				Headers: []kgo.RecordHeader{
					{Key: "h-multi", Value: []byte("hello")},
					{Key: "umh_topic", Value: []byte("umh.v1.evil.divergent")},
				},
			},
			&kgo.Record{Key: []byte(key2), Value: []byte(`{"v":2}`)},
		)

		var mu sync.Mutex
		seen := map[string]map[string]string{} // payload -> metadata snapshot
		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
`, func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, msg := range b {
				bs, _ := msg.AsBytes()
				m := map[string]string{}
				for _, k := range []string{"umh_topic", "kafka_msg_key", "kafka_key", "kafka_topic", "kafka_timestamp_ms", "h-multi"} {
					v, _ := msg.MetaGet(k)
					m[k] = v
				}
				if _, ok := msg.MetaGet("__rpcn_kafka_headers"); ok {
					m["__rpcn_kafka_headers"] = "present"
				}
				seen[string(bs)] = m
			}
			return nil
		})
		defer stop()

		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return len(seen)
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(Equal(2), "both messages must arrive through uns_beta")

		mu.Lock()
		m1, ok1 := seen[`{"v":1}`]
		m2, ok2 := seen[`{"v":2}`]
		mu.Unlock()
		Expect(ok1).To(BeTrue())
		Expect(ok2).To(BeTrue())

		Expect(m1["umh_topic"]).To(Equal(key1), "umh_topic must alias the record's own key, overriding the divergent umh_topic header")
		Expect(m1["kafka_msg_key"]).To(Equal(key1), "kafka_msg_key must alias the record's own key")
		Expect(m1["kafka_key"]).To(Equal(key1), "kafka_key must still equal the record key after aliasing")
		Expect(m1["kafka_topic"]).To(Equal("umh.messages"))
		Expect(m1["kafka_timestamp_ms"]).To(MatchRegexp(`^\d+$`), "kafka_timestamp_ms must be a numeric string")
		Expect(m1["h-multi"]).To(Equal("hello"), "record headers must pass through as metadata")
		// The delegated input's raw kgo header slice must not leak downstream:
		// the uns output copies metadata into Kafka headers, so a surviving
		// __rpcn_kafka_headers would compound per hop.
		Expect(m1).NotTo(HaveKey("__rpcn_kafka_headers"), "__rpcn_kafka_headers must be stripped before delivery")

		Expect(m2["umh_topic"]).To(Equal(key2), "umh_topic must alias the record's own key")
		Expect(m2["kafka_msg_key"]).To(Equal(key2), "kafka_msg_key must alias the record's own key")
	})

	// Documented known gap, pinned as current behavior (not aspiration): the
	// delegated redpanda input applies record headers AFTER its native fields,
	// so a producer header literally named "kafka_key" overwrites the real
	// record key in metadata before the umh_topics filter runs and before
	// ReadBatch stamps the aliases. The shadowed value therefore controls the
	// drop/deliver decision as well as the aliases. The deferral contract
	// (detection is possible at this layer, recovery of the shadowed key is
	// not) lives on the trust-boundary comment in ReadBatch
	// (uns_beta_input.go). If this test starts failing, the deferred
	// hardening has been implemented and this pin should be replaced with
	// the new contract.
	It("stamps a spoofed kafka_key header over the real record key (documented gap)", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-spoof"
		produce(GinkgoT(), addr, &kgo.Record{
			Key:   []byte("umh.v1.acme.real"),
			Value: []byte(`{"v":1}`),
			Headers: []kgo.RecordHeader{
				{Key: "kafka_key", Value: []byte("umh.v1.spoofed")},
			},
		})

		var mu sync.Mutex
		var umhTopics []string
		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
`, func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, msg := range b {
				v, _ := msg.MetaGet("umh_topic")
				umhTopics = append(umhTopics, v)
			}
			return nil
		})
		defer stop()

		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return len(umhTopics)
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(BeNumerically(">=", 1), "message never arrived through uns_beta")

		mu.Lock()
		got := umhTopics[0]
		mu.Unlock()
		Expect(got).To(Equal("umh.v1.spoofed"), "the kafka_key header shadows the record key (documented gap)")
	})

	// The data-loss direction of the same documented gap: the shadowed value
	// controls the DROP decision too. Under a selective umh_topics filter, a
	// record whose REAL key matches the filter but whose "kafka_key" header
	// does not is dropped — and the shared delegated ack then commits past it,
	// so the legitimately keyed record is gone for good (the spoof pin above
	// only covers the benign direction: a delivered record carrying the
	// spoofed alias). The matching ride-along record shares the single produce
	// call so the consumer-visible batch is — overwhelmingly likely under
	// kfake — never all-filtered: the all-filtered path is the next rung
	// (spec P8) and must not be entered here. If this test starts failing,
	// the deferred hardening has been implemented and this pin should be
	// replaced with the new contract.
	It("drops and commits past a matching record whose spoofed kafka_key header fails the filter (documented gap)", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-spoof-dataloss"
		produce(GinkgoT(), addr,
			&kgo.Record{
				// offset 0: the real key matches the filter, but the spoofed
				// header — applied AFTER the native fields by the delegated
				// input — does not, so the filter drops the record.
				Key:   []byte("umh.v1.acme.real"),
				Value: []byte(`{"v":"spoofed"}`),
				Headers: []kgo.RecordHeader{
					{Key: "kafka_key", Value: []byte("umh.v1.evil.spoofed")},
				},
			},
			// offset 1: the ride-along, plainly keyed and matching.
			rec("umh.v1.acme.ride", `{"v":"ride"}`),
		)

		var mu sync.Mutex
		var got []string
		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
  umh_topics:
    - "umh\\.v1\\.acme\\..+"
`, func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range b {
				bs, _ := m.AsBytes()
				got = append(got, string(bs))
			}
			return nil
		})
		defer stop()

		// The committed offset reaching 2 proves the shared delegated ack
		// resolved past BOTH records, the dropped spoofed one included — the
		// data loss this pin documents. By then any leaked spoofed delivery
		// has already happened, so the exclusion assertion below is checked
		// at a meaningful time. 15s timeout: the inner redpanda input commits
		// on the 5s commit_period tick pinned in the uns_beta template
		// (uns_beta_template.yaml).
		Eventually(func(g Gomega) {
			off, ok, err := committedOffsetE(addr, group)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "no offset committed yet")
			g.Expect(off).To(Equal(int64(2)), "committed offset must advance past the dropped spoofed record and the ride-along")
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		mu.Lock()
		defer mu.Unlock()
		Expect(got).To(ConsistOf(`{"v":"ride"}`),
			"the spoofed record must never be delivered: its kafka_key header shadows the matching real key in the filter decision (documented gap)")
	})
})

// The umh_topics key-regex filter: only records whose Kafka key matches one
// of the configured patterns reach the pipeline; a record with an
// empty/absent key never matches, regardless of patterns (why: see the
// betaKeyFilter.matches doc). The delegated ack covers the whole poll, so a
// batch mixing kept and dropped records must still commit past the dropped
// offsets once the kept subset is acked. (ENG-5094)
var _ = Describe("uns_beta umh_topics key filter", Label("uns_beta"), func() {
	It("delivers only matching keys, drops keyless records, and commits past the dropped offsets", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-keyfilter"
		// All three records go in ONE produce call so they share a fetch and —
		// overwhelmingly likely under kfake — one delegated batch; the public
		// surface cannot deterministically force a single batch. The matching
		// record rides alongside so the kept subset is never empty: an
		// all-filtered batch would be returned empty without its ack
		// resolving, and self-acking that case is the next rung (spec P8).
		produce(GinkgoT(), addr,
			rec("umh.v1.acme.berlin.temp", `{"v":"berlin"}`), // offset 0: matches
			rec("umh.v1.acme.munich.temp", `{"v":"munich"}`), // offset 1: dropped, key does not match
			&kgo.Record{Value: []byte(`{"v":"keyless"}`)},    // offset 2: dropped, keyless never matches
		)

		var mu sync.Mutex
		var got []string
		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
  umh_topics:
    - "umh\\.v1\\.acme\\.berlin\\..+"
`, func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range b {
				bs, _ := m.AsBytes()
				got = append(got, string(bs))
			}
			return nil
		})
		defer stop()

		// The committed offset reaching 3 proves the shared delegated ack
		// resolved for the whole poll, dropped records included — and by then
		// any leaked Munich/keyless delivery has already happened, so the
		// exclusion assertion below is checked at a meaningful time. The inner
		// redpanda input commits on the 5s commit_period tick pinned in the
		// uns_beta template (uns_beta_template.yaml), so Gomega's 1s default
		// timeout would flake.
		Eventually(func(g Gomega) {
			off, ok, err := committedOffsetE(addr, group)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "no offset committed yet")
			g.Expect(off).To(Equal(int64(3)), "committed offset must advance past the dropped Munich and keyless records")
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		mu.Lock()
		defer mu.Unlock()
		Expect(got).To(ConsistOf(`{"v":"berlin"}`),
			"only the Berlin-keyed record may reach the consumer: Munich fails the umh_topics regex and a keyless record never matches any pattern")
	})

	// The keyless contract holds even when umh_topics is OMITTED: the default
	// is [".*"], and `.*` matches "" as a regex — this pins that the
	// never-match rule overrides even the match-everything default (rationale
	// and the legacy behavior change: betaKeyFilter.matches doc).
	It("drops a keyless record under the default match-everything filter and still commits past it", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-keyless"
		// One produce call: the keyed record rides alongside so the delegated
		// batch is — overwhelmingly likely under kfake — never all-filtered
		// (the all-filtered case is the next rung, spec P8).
		produce(GinkgoT(), addr,
			rec("umh.v1.acme.keyed", `{"v":1}`),   // offset 0: keyed, delivered
			&kgo.Record{Value: []byte(`{"v":2}`)}, // offset 1: keyless, dropped
		)

		var mu sync.Mutex
		var got []string
		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
`, func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range b {
				bs, _ := m.AsBytes()
				got = append(got, string(bs))
			}
			return nil
		})
		defer stop()

		// Offset 2 proves the shared delegated ack covered the dropped keyless
		// record — and by then any leaked keyless delivery has already
		// happened, so the exclusion check below is checked at a meaningful
		// time. 15s timeout: the inner redpanda input commits on the 5s
		// commit_period tick pinned in the uns_beta template
		// (uns_beta_template.yaml).
		Eventually(func(g Gomega) {
			off, ok, err := committedOffsetE(addr, group)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "no offset committed yet")
			g.Expect(off).To(Equal(int64(2)), "committed offset must advance past the dropped keyless record")
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		mu.Lock()
		defer mu.Unlock()
		Expect(got).To(ConsistOf(`{"v":1}`),
			"the keyless record must never reach the consumer, even though the default `.*` pattern matches the empty string")
	})
})

// capturingMetricsExporter is a service.MetricsExporter that records the NAME
// of every metric the framework asks it to create into a mutex-guarded set,
// returning no-op metric instances (mirroring benthos's own mockMetricsExporter
// in public/service/metrics_test.go). It lets a test assert that a specific
// metric NAME reached the OUTER stream's metrics registry.
type capturingMetricsExporter struct {
	mu    sync.Mutex
	names map[string]bool
}

func (c *capturingMetricsExporter) record(name string) {
	c.mu.Lock()
	c.names[name] = true
	c.mu.Unlock()
}

// registered reports whether a metric of the given name was ever created on
// this exporter.
func (c *capturingMetricsExporter) registered(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.names[name]
}

func (c *capturingMetricsExporter) NewCounterCtor(name string, _ ...string) service.MetricsExporterCounterCtor {
	c.record(name)
	return func(_ ...string) service.MetricsExporterCounter { return noopMetric{} }
}

func (c *capturingMetricsExporter) NewTimerCtor(name string, _ ...string) service.MetricsExporterTimerCtor {
	c.record(name)
	return func(_ ...string) service.MetricsExporterTimer { return noopMetric{} }
}

func (c *capturingMetricsExporter) NewGaugeCtor(name string, _ ...string) service.MetricsExporterGaugeCtor {
	c.record(name)
	return func(_ ...string) service.MetricsExporterGauge { return noopMetric{} }
}

func (c *capturingMetricsExporter) Close(context.Context) error { return nil }

// noopMetric satisfies the counter/timer/gauge metric interfaces; the test only
// cares that the metric was CREATED (its name recorded), not its value.
type noopMetric struct{}

func (noopMetric) Incr(int64)          {}
func (noopMetric) IncrFloat64(float64) {}
func (noopMetric) Timing(int64)        {}
func (noopMetric) Set(int64)           {}
func (noopMetric) SetFloat64(float64)  {}

// captureExporterName is the metrics-exporter plugin name the test selects via
// SetMetricsYAML. RegisterMetricsExporter is process-global and errors if the
// same name is registered twice, so registration is guarded by a sync.Once and
// the ctor always hands back the same package-level capturer instance — robust
// to Ginkgo's randomized/parallel spec ordering.
const captureExporterName = "uns_beta_test_capture"

var (
	captureExporter     = &capturingMetricsExporter{names: map[string]bool{}}
	captureExporterOnce sync.Once
)

func registerCaptureExporter() {
	captureExporterOnce.Do(func() {
		err := service.RegisterMetricsExporter(
			captureExporterName,
			service.NewConfigSpec(),
			func(*service.ParsedConfig, *service.Logger) (service.MetricsExporter, error) {
				return captureExporter, nil
			},
		)
		if err != nil {
			panic(err)
		}
	})
}

// This is the observability regression guard for the uns_beta restructure
// (commit a2993e6f). The restructure replaced the inner redpanda input's
// construction from ParseYAML(innerYAML, nil) — which built it on a NOOP
// manager whose logs/metrics/kafka_lag were silently discarded — with a
// service.NewInputField("input"), so the FRAMEWORK builds the inner input on
// the REAL outer manager and routes its metrics through the outer stream's
// registry. ZERO behavioral tests prove this: a regression reintroducing a
// detached/noop inner manager would keep every other spec in this file green
// while silently dropping the inner input's observability.
//
// This spec closes that gap as a TRUE behavioral guard. It attaches a capturing
// metrics exporter to the OUTER StreamBuilder, runs uns_beta against the kfake
// broker, and asserts the inner redpanda input's own `redpanda_lag` gauge
// (registered by franz_reader_ordered.go only when consumer_group != "", on the
// poll goroutine that starts during Connect) reaches that outer-attached
// exporter. Under the pre-restructure noop-manager construction the inner
// input's metrics would land on a discarded registry and `redpanda_lag` would
// never reach the capturer, so this assertion would fail.
var _ = Describe("uns_beta inner-input metrics routing", Label("uns_beta"), func() {
	It("routes the inner redpanda input's redpanda_lag gauge through the outer stream's metrics registry", func() {
		registerCaptureExporter()

		addr := startBroker(GinkgoT())
		const group = "uns-beta-metrics-routing"
		produce(GinkgoT(), addr, rec("umh.v1.acme.berlin.temp", `{"v":1}`))

		sb := service.NewStreamBuilder()
		Expect(sb.AddInputYAML(`
uns_beta:
  broker_address: "` + addr + `"
  consumer_group: "` + group + `"
`)).To(Succeed())
		// Select the capturing exporter. The metrics-section YAML keys the
		// exporter by its registered plugin name (see benthos's own
		// TestMetricsPlugin in public/service/metrics_test.go); an empty config
		// body suffices since the spec has no fields.
		Expect(sb.SetMetricsYAML(captureExporterName + ": {}")).To(Succeed())

		var mu sync.Mutex
		var got int
		Expect(sb.AddBatchConsumerFunc(func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			got += len(b)
			mu.Unlock()
			return nil // ack so the inner input keeps polling/connected
		})).To(Succeed())

		stream, err := sb.Build()
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		var runErr error // written before close(done), read after <-done
		go func() { defer close(done); runErr = stream.Run(ctx) }()
		defer func() {
			cancel()
			<-done
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				Fail("stream run: " + runErr.Error())
			}
		}()

		// Connect + read at least one record so the inner input's poll
		// goroutine (which registers redpanda_lag) is running.
		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return got
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(BeNumerically(">=", 1), "message never arrived through uns_beta")

		// The routing assertion: redpanda_lag — a metric created ONLY by the
		// inner redpanda input — must have been created on the outer-attached
		// capturer. It registers on the poll goroutine started during Connect.
		Eventually(func() bool {
			return captureExporter.registered("redpanda_lag")
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(BeTrue(), "the inner redpanda input's redpanda_lag gauge never reached the outer stream's metrics registry — the inner input is being built on a detached/noop manager (the pre-restructure construction)")
	})
})

// The capstone for ENG-5094: the flipped reproduction of
// TestUNSInput_CommitsPastNACK_DataLoss, end to end through uns_beta. The old
// uns input commits the polled head past a NACKed batch (data loss); uns_beta
// must instead redeliver the NACKed message until it succeeds and commit only
// after.
var _ = Describe("uns_beta NACK redelivery capstone", Label("uns_beta"), func() {
	It("redelivers a NACKed message until success and commits the offset only after", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-capstone"
		// A occupies offset 0, B offset 1 (produce is synchronous, in call order).
		produce(GinkgoT(), addr,
			rec("umh.v1.acme.poison", `{"v":"A"}`),
			rec("umh.v1.acme.ok", `{"v":"B"}`))

		const payloadA = `{"v":"A"}`
		const failuresWanted = 3
		var mu sync.Mutex
		deliveries := map[string]int{} // payload -> delivery count
		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
`, func(_ context.Context, b service.MessageBatch) error {
			// This callback runs off the test goroutine, so a Gomega failure
			// inside it must be recovered here to be reported instead of
			// crashing the suite.
			defer GinkgoRecover()
			mu.Lock()
			defer mu.Unlock()
			reject := false
			for _, m := range b {
				bs, _ := m.AsBytes()
				deliveries[string(bs)]++
				if strings.Contains(string(bs), "A") && deliveries[string(bs)] <= failuresWanted {
					reject = true // NACK the batch containing A, first 3 times
				}
			}
			if reject {
				// auto_replay_nacks redelivers in-process within milliseconds,
				// so an external poll can never reliably observe the failing
				// window; this callback IS the failing window — while A is
				// about to be NACKed here, nothing at or past A's offset (0)
				// may be committed.
				if deliveries[payloadA] == 2 {
					off, ok, err := committedOffsetE(addr, group)
					Expect(err).NotTo(HaveOccurred())
					if ok {
						Expect(off).To(BeNumerically("<", 1),
							"offset committed past the NACKed message while it was still being rejected")
					}
				}
				return errors.New("simulated output failure")
			}
			return nil
		})
		defer stop()

		// P2: A is redelivered until it succeeds (failuresWanted rejections,
		// then one accepted delivery).
		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return deliveries[payloadA]
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(BeNumerically(">=", failuresWanted+1), "A was never redelivered to success")

		// P1: after A succeeds the committed offset reaches past both messages.
		// The inner redpanda input commits on a 5s commit_period tick, so
		// Gomega's 1s default timeout would flake.
		Eventually(func(g Gomega) {
			off, ok, err := committedOffsetE(addr, group)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "no offset committed yet")
			g.Expect(off).To(Equal(int64(2)))
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	})
})
