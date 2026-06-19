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
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	// Registers benthos core components (incl. the "none" tracer that
	// StreamBuilder.Build defaults to). The production binary gets these via
	// its full component bundle; this minimal test package only blank-imports
	// the kafka components, so the StreamBuilder build needs pure here.
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

// runUnsBetaStream starts a StreamBuilder pipeline: uns_beta input -> consumerFn.
// consumerFn's returned error NACKs the batch. Returns a stop func.
func runUnsBetaStream(t testingT, unsBetaYAML string, consumerFn func(context.Context, service.MessageBatch) error) func() {
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
	var stopOnce sync.Once
	return func() {
		// Idempotent: specs may call stop() explicitly AND via defer (e.g. a
		// restart spec stops run 1 before starting run 2). A second StopWithin on
		// an already-stopped stream would error, so guard with sync.Once.
		stopOnce.Do(func() {
			// StopWithin (not a bare context cancel) is what actually tears the inner
			// redpanda input down: stream.Run returns on ctx cancel in microseconds
			// WITHOUT waiting for the delegated franz client to stop, so a bare cancel
			// leaves a leaked consumer goroutine that keeps polling and committing on
			// the same consumer group. A restart spec that produces a record after
			// stop() would otherwise see the lingering stream-1 consumer eat and commit
			// past it (apparent data loss that is purely a teardown race, not a filter
			// bug). StopWithin drains the franz client before returning, so the group
			// is genuinely free for a restart. The streams under test self-terminate
			// quickly once their input closes; 30s is a generous teardown bound.
			stopErr := stream.StopWithin(30 * time.Second)
			cancel() // backstop: unblock Run if StopWithin's path left it parked
			<-done
			if stopErr != nil {
				t.Fatalf("stream stop: %v", stopErr)
			}
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				t.Fatalf("stream run: %v", runErr)
			}
		})
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
				if _, ok := msg.MetaGet(rpcnKafkaHeadersKey); ok {
					m[rpcnKafkaHeadersKey] = "present"
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
		// rpcnKafkaHeadersKey value would compound per hop.
		Expect(m1).NotTo(HaveKey(rpcnKafkaHeadersKey), rpcnKafkaHeadersKey+" must be stripped before delivery")

		Expect(m2["umh_topic"]).To(Equal(key2), "umh_topic must alias the record's own key")
		Expect(m2["kafka_msg_key"]).To(Equal(key2), "kafka_msg_key must alias the record's own key")
	})

	// Read-safe header typing is provided by the delegated redpanda input,
	// not by uns_beta. Connect v4.94.1's AddHeaders (franz_headers.go) stores
	// every header value uniformly: nil -> nil, empty -> "", else
	// string(h.Value). A single-byte header therefore reads back as its string
	// ("x"), with no normalization in uns_beta.
	//
	// This pins that upstream contract and guards against a future connect
	// regression that reintroduces the older v4.78 behavior: that version had
	// an extra len==1 branch storing the single byte as a rune/int32, which
	// benthos MetaGet then rendered as the decimal of the byte ("x" -> "120").
	// If connect regresses, the single-byte assertion below goes red and
	// uns_beta would need its own normalization to stay read-safe.
	//
	// The native int fields (kafka_timestamp_ms, kafka_partition) and the
	// string aliases are checked to pin that the upstream contract leaves them
	// as their decimal strings. (ENG-5094 / ENG-5105)
	It("reads single-byte, empty, and multi-byte headers back as strings without corrupting native int fields", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-header-typing"
		const key = "umh.v1.acme.headertyping"
		produce(GinkgoT(), addr,
			&kgo.Record{
				Key:   []byte(key),
				Value: []byte(`{"v":1}`),
				Headers: []kgo.RecordHeader{
					{Key: "h-multi", Value: []byte("hello")},
					{Key: "h-single", Value: []byte{'x'}},
					{Key: "h-empty", Value: []byte{}},
				},
			},
		)

		var mu sync.Mutex
		var snap map[string]string // metadata snapshot of the delivered message
		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
`, func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, msg := range b {
				m := map[string]string{}
				for _, k := range []string{"h-multi", "h-single", "h-empty", "umh_topic", "kafka_msg_key", "kafka_timestamp_ms", "kafka_partition"} {
					v, has := msg.MetaGet(k)
					if has {
						m[k] = v
					}
				}
				snap = m
			}
			return nil
		})
		defer stop()

		Eventually(func() map[string]string {
			mu.Lock()
			defer mu.Unlock()
			return snap
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			ShouldNot(BeNil(), "the headered message never arrived through uns_beta")

		mu.Lock()
		m := snap
		mu.Unlock()

		// Read-safe header typing: connect v4.94.1 stores the single-byte header
		// as the string "x", so MetaGet returns "x" and not the decimal of its
		// byte ("120"). A regression to v4.78's rune branch would flip h-single.
		Expect(m["h-multi"]).To(Equal("hello"), "multi-byte header must read back as its string")
		Expect(m["h-single"]).To(Equal("x"), "single-byte header must read back as its string, not the decimal of its byte")
		Expect(m).To(HaveKey("h-empty"), "empty header must still appear in metadata")
		Expect(m["h-empty"]).To(Equal(""), "empty header must read back as the empty string")

		// Native-field guard: the upstream contract leaves the native int
		// metadata as decimal strings. Pin that they are not coerced via
		// string(rune) into control characters.
		Expect(m["kafka_timestamp_ms"]).To(MatchRegexp(`^\d+$`), "kafka_timestamp_ms must stay a decimal string")
		Expect(m).To(HaveKey("kafka_partition"))
		Expect(m["kafka_partition"]).To(Equal("0"), "kafka_partition must stay its decimal string, not a control character")

		// Alias semantics unchanged: the aliases still equal the record key.
		Expect(m["umh_topic"]).To(Equal(key), "umh_topic alias must equal the record key")
		Expect(m["kafka_msg_key"]).To(Equal(key), "kafka_msg_key alias must equal the record key")
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
	// kfake — never all-filtered, keeping this pin on the mixed-batch path
	// (the all-filtered self-ack — advancing the offset past an entirely
	// non-matching poll — is covered by its own spec). If this
	// test starts failing, the deferred hardening has been implemented and this
	// pin should be replaced with the new contract.
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
		// record rides alongside so the kept subset is never empty, keeping
		// this spec on the mixed-batch path; the all-filtered case (where
		// ReadBatch self-acks the empty batch so the offset still advances) has
		// its own spec.
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
		// (the all-filtered case, where ReadBatch self-acks the empty batch so
		// the offset still advances, has its own spec).
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

// An all-filtered poll — every record in the fetch fails the umh_topics filter
// — must still advance the committed offset. Benthos's AsyncReader
// discards an empty MessageBatch WITHOUT calling its AckFunc, so the delegated
// redpanda input's checkpoint never resolves and the partition wedges: at high
// selectivity nearly every fetch is all-filtered, so a selective consumer hangs
// at offset 0 forever. uns_beta must self-ack the delegated transaction on an
// all-filtered poll so the commit advances even though the consumer never sees a
// message. (ENG-5094 / ENG-5105)
var _ = Describe("uns_beta all-filtered poll commit", Label("uns_beta"), func() {
	It("self-acks an all-filtered poll so the committed offset advances while the consumer receives nothing", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-all-filtered"
		// Three records, none of whose keys match the select-1 filter below: this
		// is the all-filtered poll. The consumer never sees a message, yet the
		// offset must still advance to 3 — otherwise the partition wedges.
		produce(GinkgoT(), addr,
			rec("umh.v1.acme.berlin.temp", `{"v":0}`),
			rec("umh.v1.acme.munich.temp", `{"v":1}`),
			rec("umh.v1.acme.hamburg.temp", `{"v":2}`))

		var mu sync.Mutex
		var got []string
		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
  umh_topics:
    - "^only-this$"
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

		// End-to-end effect check: the committed offset advances past all three
		// non-matching records even though no message ever reaches the consumer.
		// The self-ack MECHANISM itself is pinned by the broker-free unit specs
		// (uns_beta_input_test.go asserts inner.acked); this spec confirms the
		// observable end-to-end result. The inner redpanda input commits on the
		// 5s commit_period tick pinned in the uns_beta template, so the 15s
		// timeout is required.
		Eventually(func(g Gomega) {
			off, ok, err := committedOffsetE(addr, group)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "no offset committed yet — the all-filtered poll wedged the partition")
			g.Expect(off).To(Equal(int64(3)), "committed offset must advance past all three non-matching records")
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		mu.Lock()
		defer mu.Unlock()
		Expect(got).To(BeEmpty(), "no record matches ^only-this$, so the consumer must receive nothing")
	})

	// A production-shaped selective consumer: thousands of distinct keys, only
	// one of which matches the filter. At this selectivity nearly every fetch is
	// all-filtered, so before the self-ack landed the stream wedged at offset 0
	// (zero delivered) — this is the real ENG-5105 use case the self-ack
	// unblocks. The acceptance gate: the stream DRAINS (committed offset reaches
	// the high-water mark) and the one matching record is delivered.
	It("drains a high-cardinality selective stream to completion, delivering only the matching record", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-selective-drain"
		const total = 2000
		const matchAt = 1234 // the single matching record's offset

		records := make([]*kgo.Record, total)
		for i := 0; i < total; i++ {
			if i == matchAt {
				records[i] = rec("umh.v1.match.only", `{"v":"match"}`)
				continue
			}
			records[i] = rec(fmt.Sprintf("umh.v1.nomatch.k%d", i), `{"v":"drop"}`)
		}
		produce(GinkgoT(), addr, records...)

		var mu sync.Mutex
		var got []string
		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
  umh_topics:
    - "^umh\\.v1\\.match\\..+$"
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

		// The committed offset reaches the high-water mark (total): the stream
		// drained instead of wedging at the first all-filtered fetch. Draining
		// 2000 self-acks then waiting for a 5s commit tick (commit_period is
		// hardcoded in the template and cannot be shortened from test config) can
		// eat most of a 15s budget on a slow kfake startup, so this single spec
		// gets a 30s timeout to keep it off the merge-queue flake list.
		Eventually(func(g Gomega) {
			off, ok, err := committedOffsetE(addr, group)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "no offset committed yet — the selective stream wedged")
			g.Expect(off).To(Equal(int64(total)), "committed offset must reach the high-water mark, draining all non-matching records")
		}).WithTimeout(30 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		mu.Lock()
		defer mu.Unlock()
		Expect(got).To(ConsistOf(`{"v":"match"}`), "only the single matching record may reach the consumer")
	})
})

// A fresh consumer group must read from the EARLIEST offset: records produced
// BEFORE the consumer ever started must still be delivered. The template pins
// start_offset = "earliest" (uns_beta_template.yaml); the config-value half is
// pinned by Describe("uns_beta template rendering") in uns_beta_input_test.go
// (asserts start_offset == "earliest"), this spec pins the observable BEHAVIOR.
// A "latest" start would
// position a never-committed group at the high-water mark, so a record produced
// before the group connected would be skipped — the arrival assertion below
// would time out. (ENG-5094 / spec P5)
var _ = Describe("uns_beta start-from-earliest", Label("uns_beta"), func() {
	It("delivers records produced before a fresh consumer group started", func() {
		addr := startBroker(GinkgoT())
		// A never-committed group name (unique suffix) so there is no committed
		// offset to resume from — start_offset alone decides where it begins.
		group := fmt.Sprintf("uns-beta-earliest-%d", time.Now().UnixNano())

		// Produce BOTH records FIRST, before any consumer exists. Their keys
		// match the umh_topics filter so they are delivered, not dropped.
		produce(GinkgoT(), addr,
			rec("umh.v1.enterprise.siteA._raw.temp", `{"v":"A"}`),
			rec("umh.v1.enterprise.siteB._raw.temp", `{"v":"B"}`))

		var mu sync.Mutex
		var got []string
		// THEN start the stream with the fresh group.
		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
  umh_topics:
    - "^umh\\.v1\\."
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

		// Both pre-existing records must arrive. A fresh "latest" group would
		// never see them (it would position past them at connect), so this
		// arrival assertion is what distinguishes earliest from latest.
		Eventually(func() []string {
			mu.Lock()
			defer mu.Unlock()
			return append([]string(nil), got...)
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(ContainElements(`{"v":"A"}`, `{"v":"B"}`),
				"both records produced before the fresh consumer group started must be delivered (start_offset = earliest)")
	})
})

// The filter × ack-forward × NACK-redelivery interaction that the mixed-batch
// ack spec and the unfiltered capstone each cover only half of. A mixed
// delegated poll (one matching + one non-matching record) whose KEPT record the
// consumer NACKs: auto_replay_nacks (on the INNER redpanda input, pre-filter)
// must redeliver the full original poll, uns_beta re-applies the drop, and the
// offset must NOT commit past the NACKed kept record.
//
// The load-bearing check is the trailing positive one: the offset advances past
// both records ONLY after the kept record finally succeeds. A synchronous
// in-callback "nothing committed yet" assertion was deliberately NOT added: the
// inner redpanda input uses AutoCommitMarks + a 5s commit tick, so an ack only
// sets an in-memory mark and the broker-visible offset cannot move until the
// tick fires — a broker OffsetFetch from inside the callback would read "not
// committed" whether the ack was pending OR fired prematurely, so it cannot
// distinguish correct from buggy. The premature-ack ordering is pinned instead
// by the broker-free self-ack unit specs (uns_beta_input_test.go).
var _ = Describe("uns_beta mixed-batch NACK redelivery", Label("uns_beta"), func() {
	It("redelivers the NACKed kept record without committing past it and re-drops the filtered one", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-mixed-nack"
		// One produce call: matching (offset 0) + non-matching (offset 1) share
		// one fetch, overwhelmingly likely one delegated batch under kfake.
		produce(GinkgoT(), addr,
			rec("umh.v1.acme.keep", `{"v":"keep"}`), // offset 0: matches, the kept record
			rec("umh.v1.evil.drop", `{"v":"drop"}`), // offset 1: dropped by the filter
		)

		var mu sync.Mutex
		deliveries := map[string]int{} // payload -> delivery count
		const failuresWanted = 2

		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
  umh_topics:
    - "^umh\\.v1\\.acme\\..+$"
`, func(_ context.Context, b service.MessageBatch) error {
			defer GinkgoRecover()
			mu.Lock()
			defer mu.Unlock()
			// Only the matching record survives the filter; the dropped one is
			// re-dropped by uns_beta on every replay (it never reaches here).
			Expect(b).To(HaveLen(1), "only the kept record may reach the consumer")
			bs, _ := b[0].AsBytes()
			Expect(string(bs)).To(Equal(`{"v":"keep"}`), "the filtered record must never be delivered, even on replay")
			deliveries[string(bs)]++

			if deliveries[`{"v":"keep"}`] <= failuresWanted {
				return errors.New("simulated output failure")
			}
			return nil
		})
		defer stop()

		// Redelivery: the kept record is retried until it succeeds — no loss.
		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return deliveries[`{"v":"keep"}`]
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(BeNumerically(">=", failuresWanted+1), "the NACKed kept record was never redelivered to success")

		// Only after success does the offset commit past BOTH records (the kept
		// one succeeded; the filtered one rode the same delegated ack).
		Eventually(func(g Gomega) {
			off, ok, err := committedOffsetE(addr, group)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "offset never committed after the kept record succeeded")
			g.Expect(off).To(Equal(int64(2)), "the offset commits past both records only after the kept record succeeds")
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		mu.Lock()
		defer mu.Unlock()
		Expect(deliveries[`{"v":"drop"}`]).To(BeZero(), "the filtered record must never be delivered, on first poll or replay")
	})
})

// The mandatory safety gate: an explicit stop-and-restart across a long
// non-matching run, proving no data loss and no stuck commit. Everything above
// runs one stream to completion; nothing pins that a SECOND stream on the same
// consumer group resumes from the committed offset rather than replaying acked
// data. This is the durability claim of the whole filter design: the high-water
// placeholder advances the commit over an omitted run so the re-read window stays
// bounded, and a restart sees only data produced after the committed offset.
//
// Shape of the log (single produce, so offsets are call order):
//   - offset 0:      matching  -> delivered in run 1
//   - offsets 1..N:  non-matching -> all omitted in connect; the match is NOT
//     last, so the high-water placeholder path is exercised, and N
//     is large enough that whole fetches land all-non-matching
//     (the all-non-match high-water case).
//
// A specific umh_topics pattern (NOT ".*") is used so the non-matches are
// genuinely omitted in connect, not merely dropped by uns_beta's keyless guard.
// (ENG-5094 / ENG-5105)
var _ = Describe("uns_beta gapless restart", Label("uns_beta"), func() {
	It("resumes a restarted consumer group from the committed offset with no loss and no replay across a long omitted run", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-gapless-restart"
		const nonMatching = 1500 // a long omitted run, spanning many fetches
		const total = 1 + nonMatching

		// Offset 0 matches; offsets 1..1500 do not. The match is first, never
		// last, so the kept subset is exhausted long before the high-water mark
		// and the placeholder must carry the commit the rest of the way.
		records := make([]*kgo.Record, total)
		records[0] = rec("umh.v1.match.first", `{"v":"first"}`)
		for i := 1; i < total; i++ {
			records[i] = rec(fmt.Sprintf("umh.v1.nomatch.k%d", i), `{"v":"drop"}`)
		}
		produce(GinkgoT(), addr, records...)

		streamYAML := `
uns_beta:
  broker_address: "` + addr + `"
  consumer_group: "` + group + `"
  umh_topics:
    - "^umh\\.v1\\.match\\..+$"
`

		// --- Run 1: drain the whole log on group G ---
		var mu sync.Mutex
		var got []string
		noPlaceholder := true // set false if any message arrives without a kafka_key
		stop := runUnsBetaStream(GinkgoT(), streamYAML, func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range b {
				bs, _ := m.AsBytes()
				got = append(got, string(bs))
				// A high-water placeholder is a no-metadata record (no kafka_key).
				// It rides the ack-gated batch to advance the commit but must be
				// dropped before the consumer ever sees it. If one leaks, the
				// selective consumer would receive empty/keyless junk.
				if _, ok := m.MetaGet("kafka_key"); !ok {
					noPlaceholder = false
				}
			}
			return nil
		})
		defer stop()

		// The committed offset reaches the high-water mark even though all but one
		// record were omitted: this is the proof that the placeholder advances the
		// commit so the re-read window is bounded (no stuck commit). Draining 1500
		// omitted records then waiting for the 5s commit_period tick (hardcoded in
		// the template, not shortenable from test config) can eat most of a 15s
		// budget on a slow kfake startup, so this gets a 30s timeout.
		Eventually(func(g Gomega) {
			off, ok, err := committedOffsetE(addr, group)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "no offset committed yet — the long omitted run wedged the partition")
			g.Expect(off).To(Equal(int64(total)), "committed offset must reach the high-water mark, proving the placeholder advanced the commit over the omitted run")
		}).WithTimeout(30 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		mu.Lock()
		Expect(got).To(ConsistOf(`{"v":"first"}`), "only the single matching record may reach the consumer in run 1")
		Expect(noPlaceholder).To(BeTrue(), "no high-water placeholder / keyless / no-kafka_key record may ever reach the consumer")
		mu.Unlock()

		// --- Restart: stop run 1, produce a SECOND match after the gap ---
		// (Modeling a kill as stop-then-start on the same group is what the public
		// stream lifecycle allows; the load-bearing assertion is that run 2 resumes
		// from the committed offset, which is identical under a true mid-stream
		// kill.)
		stop()
		produce(GinkgoT(), addr, rec("umh.v1.match.second", `{"v":"second"}`)) // offset == total

		// --- Run 2: a NEW stream on the SAME group G ---
		var mu2 sync.Mutex
		var got2 []string
		stop2 := runUnsBetaStream(GinkgoT(), streamYAML, func(_ context.Context, b service.MessageBatch) error {
			mu2.Lock()
			defer mu2.Unlock()
			for _, m := range b {
				bs, _ := m.AsBytes()
				got2 = append(got2, string(bs))
			}
			return nil
		})
		defer stop2()

		// Run 2 must deliver ONLY the new record. If it resumed from 0 (lost
		// commit) the first match would reappear; if the placeholder had been
		// committed past an un-acked record, the new record could be skipped.
		Eventually(func() []string {
			mu2.Lock()
			defer mu2.Unlock()
			return append([]string(nil), got2...)
		}).WithTimeout(30*time.Second).WithPolling(100*time.Millisecond).
			Should(ConsistOf(`{"v":"second"}`),
				"the restarted group must deliver only the record produced after the committed offset — no replay of the acked first match, no loss of the new one")

		// And the commit advances to cover the new record too.
		Eventually(func(g Gomega) {
			off, ok, err := committedOffsetE(addr, group)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue())
			g.Expect(off).To(Equal(int64(total+1)), "committed offset must advance past the post-restart record")
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	})
})

// closedLocalPort binds a loopback listener to grab an OS-assigned port, then
// closes it — the returned "127.0.0.1:PORT" is a port nothing is listening on,
// so a dial against it is refused (a guaranteed-closed port, not merely an
// "unlikely to be open" one).
func closedLocalPort(t testingT) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve closed port: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close reserved port: %v", err)
	}
	return addr
}

// Connect must FAIL FAST against an unreachable broker rather than starting a
// stream that delivers nothing. The broker-free unit specs (uns_beta_input_test.go)
// fabricate the ConnectionTest result; this spec exercises the REAL probe path —
// it builds a uns_beta_reader against a closed localhost port and asserts the
// production Connect (delegated FranzReaderUnordered.ConnectionTest → kgo ping)
// returns a non-nil error, bounded by connectProbeTimeout (F2). The redpanda
// input is built on a real manager via ConfigSpec.ParseYAML, so this is the same
// inner OwnedInput.ConnectionTest the deployed input runs, not a fake.
// (ENG-5094 / ENG-5105)
var _ = Describe("uns_beta Connect against an unreachable broker", Label("uns_beta"), func() {
	It("returns a Connect error within the connect-probe bound", func() {
		addr := closedLocalPort(GinkgoT())

		// Build the uns_beta_reader directly so Connect can be called on the real
		// input. The nested redpanda block mirrors what the uns_beta template
		// renders (uns_beta_template.yaml) for the closed-port broker; the
		// top-level normalized scalars match it by construction, as the template
		// guarantees. ParseYAML builds the inner redpanda input on a real manager,
		// so its ConnectionTest is the production ping.
		readerYAML := `
input:
  redpanda:
    seed_brokers: ["` + addr + `"]
    topics: ["umh.messages"]
    consumer_group: "uns-beta-unreachable"
    start_offset: "earliest"
seed_brokers: ["` + addr + `"]
consumer_group: "uns-beta-unreachable"
kafka_topic: "umh.messages"
umh_topics: [".*"]
`
		pConf, err := unsBetaReaderConfigSpec().ParseYAML(readerYAML, service.GlobalEnvironment())
		Expect(err).NotTo(HaveOccurred(), "the reader config must parse")

		input, err := newUnsBetaReader(pConf, service.MockResources())
		Expect(err).NotTo(HaveOccurred(), "the reader must construct against a (closed) broker")
		defer func() {
			// The connectivity probe spins up the inner redpanda reader, whose
			// retry loop keeps dialing the closed port; OwnedInput.Close waits for
			// that reader to stop and would otherwise block the spec. Bound the
			// close — a leaked dial goroutine after the deadline is acceptable in
			// this no-goleak suite; the assertion below is what the spec pins.
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer closeCancel()
			_ = input.Close(closeCtx)
		}()

		// Run Connect off the spec goroutine so a HUNG probe (the bug F2 guards)
		// trips the Eventually deadline and fails loudly, while a correctly bounded
		// probe returns its refusal error well inside connectProbeTimeout.
		done := make(chan error, 1)
		go func() { done <- input.Connect(context.Background()) }()

		var connErr error
		Eventually(done, connectProbeTimeout+5*time.Second, 50*time.Millisecond).
			Should(Receive(&connErr), "Connect must return within the connect-probe bound, not hang")
		Expect(connErr).To(HaveOccurred(),
			"Connect against a closed port must surface a real connection error, not start a stream that delivers nothing")
	})
})

// capturingMetricsExporter is a service.MetricsExporter that records the NAME
// of every metric the framework asks it to create, AND accumulates the summed
// Incr of every counter keyed by name, into mutex-guarded maps (mirroring
// benthos's own mockMetricsExporter in public/service/metrics_test.go). It lets
// a test assert both that a metric NAME reached the OUTER stream's metrics
// registry and, for counters, the VALUE that was incremented — so a counter
// that is named but never incremented (or incremented by the wrong amount) is
// caught, not passed.
type capturingMetricsExporter struct {
	mu     sync.Mutex
	names  map[string]bool
	counts map[string]int64
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

// counterValue returns the summed Incr of the named counter (0 if the counter
// was never created or never incremented).
func (c *capturingMetricsExporter) counterValue(name string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[name]
}

// reset clears the recorded names and counter values. The capturer is a
// process-global singleton (RegisterMetricsExporter is process-wide), so a spec
// that asserts a counter value must reset first or it would observe increments
// leaked from a prior spec.
func (c *capturingMetricsExporter) reset() {
	c.mu.Lock()
	c.names = map[string]bool{}
	c.counts = map[string]int64{}
	c.mu.Unlock()
}

func (c *capturingMetricsExporter) addCount(name string, delta int64) {
	c.mu.Lock()
	c.counts[name] += delta
	c.mu.Unlock()
}

func (c *capturingMetricsExporter) NewCounterCtor(name string, _ ...string) service.MetricsExporterCounterCtor {
	c.record(name)
	return func(_ ...string) service.MetricsExporterCounter {
		return &accumulatingCounter{exporter: c, name: name}
	}
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

// accumulatingCounter is a counter metric instance that sums every Incr into the
// exporter's per-name total, so a spec can assert the incremented value (not just
// that the counter was created).
type accumulatingCounter struct {
	exporter *capturingMetricsExporter
	name     string
}

func (a *accumulatingCounter) Incr(v int64)          { a.exporter.addCount(a.name, v) }
func (a *accumulatingCounter) IncrFloat64(v float64) { a.exporter.addCount(a.name, int64(v)) }

// noopMetric satisfies the timer and gauge metric interfaces; those specs only
// care that the metric was CREATED (its name recorded), not its value.
type noopMetric struct{}

func (noopMetric) Timing(int64) {}
func (noopMetric) Set(int64)    {}

// captureExporterName is the metrics-exporter plugin name the test selects via
// SetMetricsYAML. RegisterMetricsExporter is process-global and errors if the
// same name is registered twice, so registration is guarded by a sync.Once and
// the ctor always hands back the same package-level capturer instance — robust
// to Ginkgo's randomized/parallel spec ordering.
const captureExporterName = "uns_beta_test_capture"

var (
	captureExporter     = &capturingMetricsExporter{names: map[string]bool{}, counts: map[string]int64{}}
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

// Dropped records must be observable. With the connect key_pattern pre-filter
// (ENG-5105) a record whose native key fails the pattern is OMITTED in connect,
// before it ever reaches uns_beta's filterAndAlias — so it can no longer touch
// uns_beta's per-reason counters. Decision (a): those omitted records are
// instead counted COARSELY by connect's key_omitted_records counter (on the same
// outer stream metrics registry), and uns_beta's filtered_records now counts
// ONLY records that REACH uns_beta (their native key matched the pattern) and
// that uns_beta itself drops. Because the connect pre-filter is a faithful
// superset of uns_beta's keep set, a real-keyed record that reaches uns_beta has
// already matched, so filtered_records stays at 0 for these scenarios while
// key_omitted_records carries the count. This spec drives an all-non-matching
// poll of 3 records and asserts key_omitted_records reaches 3 on the outer
// stream's metrics registry, with filtered_records left at 0. (ENG-5094 /
// ENG-5105)
var _ = Describe("uns_beta filtered_records counter", Label("uns_beta"), func() {
	It("counts records omitted by the connect pre-filter under key_omitted_records, leaving uns_beta's filtered_records at 0", func() {
		registerCaptureExporter()
		// The capturer is a process-global singleton; reset so the counter value
		// asserted below reflects only this spec's drops, not increments leaked
		// from a prior spec.
		captureExporter.reset()

		addr := startBroker(GinkgoT())
		const group = "uns-beta-filtered-counter"
		// An all-non-matching poll: none of these keys match the select-1 filter
		// below, so connect omits every one of them before message construction
		// (M=3 omitted) and uns_beta never sees them.
		produce(GinkgoT(), addr,
			rec("umh.v1.acme.berlin.temp", `{"v":0}`),
			rec("umh.v1.acme.munich.temp", `{"v":1}`),
			rec("umh.v1.acme.hamburg.temp", `{"v":2}`))
		const wantOmitted = 3

		sb := service.NewStreamBuilder()
		Expect(sb.AddInputYAML(`
uns_beta:
  broker_address: "` + addr + `"
  consumer_group: "` + group + `"
  umh_topics:
    - "^only-this$"
`)).To(Succeed())
		Expect(sb.SetMetricsYAML(captureExporterName + ": {}")).To(Succeed())

		Expect(sb.AddBatchConsumerFunc(func(_ context.Context, _ service.MessageBatch) error {
			return nil
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

		// The connect key_omitted_records counter on the outer-attached capturer
		// must be incremented by exactly the 3 omitted records. Asserting the
		// VALUE (not just that the counter was created) pins the Incr path:
		// deleting the Incr, or miscounting the omitted run, leaves it at 0 and
		// fails here.
		Eventually(func() int64 {
			return captureExporter.counterValue("key_omitted_records")
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(Equal(int64(wantOmitted)), "the connect pre-filter's key_omitted_records counter did not reach the omitted-record count on the outer stream's metrics registry — pre-filtered records are unobservable")

		// The omitted records are committed past once (the last-record placeholder
		// carries the high-water offset), so the count settles at exactly 3, not
		// climbing from re-polls.
		Consistently(func() int64 {
			return captureExporter.counterValue("key_omitted_records")
		}).WithTimeout(2*time.Second).WithPolling(200*time.Millisecond).
			Should(Equal(int64(wantOmitted)), "key_omitted_records over-counted — the same omitted records were counted more than once")

		// uns_beta's per-reason filtered_records must stay 0: none of these records
		// reached uns_beta (connect omitted them all), so uns_beta dropped nothing
		// of its own. Decision (a): per-reason counters account only for records
		// that REACH uns_beta.
		Expect(captureExporter.counterValue("filtered_records")).To(Equal(int64(0)),
			"records omitted by the connect pre-filter must not reach uns_beta's filtered_records counter")
	})

	// A MIXED batch: N matching + M non-matching keyed records under a select-some
	// filter. With the connect pre-filter, the M non-matching records are OMITTED
	// in connect (counted by key_omitted_records) and never reach uns_beta; the N
	// matching records are delivered. Because the connect pre-filter is a faithful
	// superset of uns_beta's keep set, every record that reaches uns_beta has
	// already matched, so uns_beta's own filtered_records stays at 0. This spec is
	// the mixed-batch counterpart that pins key_omitted_records to exactly M (not
	// N+M and not 0), proving the connect coarse count is per-omitted-record, not
	// per-batch or per-poll. (ENG-5094 / ENG-5105)
	It("counts only the omitted records (M) under key_omitted_records, delivers the N matching records, and leaves uns_beta's per-reason counters at 0", func() {
		registerCaptureExporter()
		captureExporter.reset()

		addr := startBroker(GinkgoT())
		const group = "uns-beta-mixed-counter"
		// N=2 matching (umh.v1.keep.*) + M=3 non-matching, all keyed. The LAST
		// produced record (highest offset) is a MATCHING one on purpose: connect
		// always keeps the last record of a poll as the high-water placeholder, and
		// when that last record does NOT match it is carried to uns_beta as an
		// empty (no-kafka_key) message that uns_beta classifies as keyless. Ending
		// on a matching record keeps the placeholder a real delivered record, so
		// this spec's per-reason-counters-stay-0 assertion is not muddied by the
		// placeholder edge (that edge is its own observable; see below).
		produce(GinkgoT(), addr,
			rec("umh.v1.keep.alpha", `{"v":0}`),   // delivered (matches pattern)
			rec("umh.v1.drop.beta", `{"v":1}`),    // omitted in connect
			rec("umh.v1.drop.delta", `{"v":2}`),   // omitted in connect
			rec("umh.v1.drop.epsilon", `{"v":3}`), // omitted in connect
			rec("umh.v1.keep.gamma", `{"v":4}`))   // delivered (matches; also the high-water placeholder)
		const wantDelivered = 2
		const wantOmitted = 3

		var mu sync.Mutex
		var delivered int
		sb := service.NewStreamBuilder()
		Expect(sb.AddInputYAML(`
uns_beta:
  broker_address: "` + addr + `"
  consumer_group: "` + group + `"
  umh_topics:
    - "^umh\\.v1\\.keep\\..+$"
`)).To(Succeed())
		Expect(sb.SetMetricsYAML(captureExporterName + ": {}")).To(Succeed())
		Expect(sb.AddBatchConsumerFunc(func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			delivered += len(b)
			mu.Unlock()
			return nil
		})).To(Succeed())

		stream, err := sb.Build()
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		var runErr error
		go func() { defer close(done); runErr = stream.Run(ctx) }()
		defer func() {
			cancel()
			<-done
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				Fail("stream run: " + runErr.Error())
			}
		}()

		// The 2 matching records must be delivered.
		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return delivered
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(Equal(wantDelivered), "the matching records must be delivered")

		// key_omitted_records must reach exactly M (the omitted subset), not N+M
		// (would mean matching records were omitted too) and not 0.
		Eventually(func() int64 {
			return captureExporter.counterValue("key_omitted_records")
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(Equal(int64(wantOmitted)), "key_omitted_records must count only the M omitted records, not the whole batch")

		// And it must SETTLE at M — a mutant counting the whole batch would show
		// N+M here.
		Consistently(func() int64 {
			return captureExporter.counterValue("key_omitted_records")
		}).WithTimeout(2*time.Second).WithPolling(200*time.Millisecond).
			Should(Equal(int64(wantOmitted)), "key_omitted_records over- or under-counted the mixed batch")

		// Every record that REACHED uns_beta had already matched the pattern (the
		// connect pre-filter is a superset of uns_beta's keep set), so uns_beta
		// dropped nothing of its own — all three per-reason counters stay 0.
		Expect(captureExporter.counterValue("filtered_records")).To(Equal(int64(0)),
			"records omitted by the connect pre-filter must not reach uns_beta's filtered_records counter")
		Expect(captureExporter.counterValue("dropped_keyless")).To(Equal(int64(0)),
			"no record reaching uns_beta was keyless")
		Expect(captureExporter.counterValue("dropped_spoofed_key")).To(Equal(int64(0)),
			"no record reaching uns_beta carried a spoof header")
	})
})

// dropped_keyless counts records with an empty/absent Kafka key that REACH
// uns_beta and fail its `key != ""` guard. With the connect key_pattern
// pre-filter (ENG-5105) a keyless record only reaches uns_beta when the pattern
// matches the empty key — i.e. under a `.*` umh_topics (".*" matches ""). So this
// spec uses `.*`: connect BUILDS the keyless record (it matched), it reaches
// uns_beta, hits the `key != ""` guard in betaKeyFilter.matches, and increments
// dropped_keyless. Under a SPECIFIC pattern the keyless record would instead be
// OMITTED in connect (its empty key fails the pattern) and counted coarsely by
// key_omitted_records — decision (a) — never reaching dropped_keyless; that path
// is exercised by the filtered_records specs above. This spec pins the
// keyless-reaches-uns_beta path that the `.*` (match-everything) deployment still
// has. (ENG-5094 / ENG-5105)
var _ = Describe("uns_beta dropped_keyless counter", Label("uns_beta"), func() {
	It("increments dropped_keyless for a keyless record that reaches uns_beta under a match-everything pattern", func() {
		registerCaptureExporter()
		captureExporter.reset()

		addr := startBroker(GinkgoT())
		const group = "uns-beta-keyless-counter"
		// A keyless record: nil key. Under the `.*` pattern below connect matches
		// it (".*" matches the empty key) and BUILDS it, so it reaches uns_beta,
		// where the `key != ""` guard drops it as keyless. A keyed record gives the
		// stream something to deliver so the poll goroutine stays live.
		produce(GinkgoT(), addr,
			&kgo.Record{Key: nil, Value: []byte(`{"v":0}`)},
			rec("umh.v1.keep.live", `{"v":1}`))

		var mu sync.Mutex
		var delivered int
		sb := service.NewStreamBuilder()
		Expect(sb.AddInputYAML(`
uns_beta:
  broker_address: "` + addr + `"
  consumer_group: "` + group + `"
  umh_topics:
    - ".*"
`)).To(Succeed())
		Expect(sb.SetMetricsYAML(captureExporterName + ": {}")).To(Succeed())
		Expect(sb.AddBatchConsumerFunc(func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			delivered += len(b)
			mu.Unlock()
			return nil
		})).To(Succeed())

		stream, err := sb.Build()
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		var runErr error
		go func() { defer close(done); runErr = stream.Run(ctx) }()
		defer func() {
			cancel()
			<-done
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				Fail("stream run: " + runErr.Error())
			}
		}()

		Eventually(func() int64 {
			return captureExporter.counterValue("dropped_keyless")
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(Equal(int64(1)), "the keyless drop must increment dropped_keyless on the outer stream's metrics registry")

		// The keyless record carried a real (absent) key, not a spoof header, and
		// the keyed record was delivered, not filtered.
		Expect(captureExporter.counterValue("dropped_spoofed_key")).To(Equal(int64(0)),
			"a keyless record must not count as spoofed")
		Expect(captureExporter.counterValue("filtered_records")).To(Equal(int64(0)),
			"a keyless record must not count as a filtered (real-keyed) drop")
		// Under `.*` connect omits nothing (every key, including the empty one,
		// matches), so the connect coarse counter never fires — the keyless record
		// is accounted for by uns_beta's dropped_keyless, not key_omitted_records.
		Expect(captureExporter.counterValue("key_omitted_records")).To(Equal(int64(0)),
			"under a match-everything pattern connect omits nothing, so key_omitted_records must stay 0")
		mu.Lock()
		Expect(delivered).To(Equal(1), "the keyed matching record must be delivered")
		mu.Unlock()
	})
})

// dropped_spoofed_key counts records carrying a foreign producer header literally
// named "kafka_key" — the header shadows the native key in the delegated input's
// post-header metadata, so uns_beta drops on the shadowed value. With the connect
// key_pattern pre-filter (ENG-5105), the spoof vector that REACHES uns_beta — and
// so reaches this counter — is the one whose NATIVE key MATCHES the pattern (so
// connect builds and delivers the record) while the spoofed kafka_key header
// FAILS the pattern: uns_beta reads the shadowed (spoofed) kafka_key, drops it,
// and the raw header slice (rpcnKafkaHeadersKey) classifies it as spoofed even
// though its post-header key is the shadowing value. This spec drives that
// matching-native / failing-spoof record and asserts dropped_spoofed_key reaches
// it.
//
// SECURITY — the other spoof vector is now CLOSED AT THE SOURCE: a record whose
// NATIVE key does NOT match the pattern but carries a MATCHING spoofed kafka_key
// header (the ENG-5125 spoof-DELIVER, where a foreign producer forges a kafka_key
// to get a non-matching record delivered under a forged umh_topic) can no longer
// be delivered: connect omits it on its non-matching NATIVE key before the
// delegated input ever applies the header, so the forged header never gets a
// chance to shadow the key. That omitted record is counted coarsely as
// key_omitted_records, NOT flagged as a spoof — a security improvement (the spoof
// can no longer land a delivery) with a quieter signal (it lands in the coarse
// omit count rather than dropped_spoofed_key). The matching-native spoof below is
// the residual vector this counter still observes. (ENG-5094 / ENG-5105 /
// ENG-5125)
var _ = Describe("uns_beta dropped_spoofed_key counter", Label("uns_beta"), func() {
	It("increments dropped_spoofed_key for a matching-native record whose spoofed kafka_key header fails the pattern", func() {
		registerCaptureExporter()
		captureExporter.reset()

		addr := startBroker(GinkgoT())
		const group = "uns-beta-spoofed-counter"
		// A record whose NATIVE key MATCHES the pattern (so connect builds and
		// delivers it to uns_beta) carrying a foreign "kafka_key" header whose value
		// FAILS the pattern. The delegated input applies the header after the native
		// fields, shadowing kafka_key with the failing spoofed value; uns_beta drops
		// on that value and the raw header slice classifies it as spoofed. A keyed
		// matching record keeps the stream delivering.
		produce(GinkgoT(), addr,
			&kgo.Record{
				Key:     []byte("umh.v1.keep.native"),
				Value:   []byte(`{"v":0}`),
				Headers: []kgo.RecordHeader{{Key: "kafka_key", Value: []byte("umh.v1.drop.spoofed")}},
			},
			rec("umh.v1.keep.live", `{"v":1}`))

		var mu sync.Mutex
		var delivered int
		sb := service.NewStreamBuilder()
		Expect(sb.AddInputYAML(`
uns_beta:
  broker_address: "` + addr + `"
  consumer_group: "` + group + `"
  umh_topics:
    - "^umh\\.v1\\.keep\\..+$"
`)).To(Succeed())
		Expect(sb.SetMetricsYAML(captureExporterName + ": {}")).To(Succeed())
		Expect(sb.AddBatchConsumerFunc(func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			delivered += len(b)
			mu.Unlock()
			return nil
		})).To(Succeed())

		stream, err := sb.Build()
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		var runErr error
		go func() { defer close(done); runErr = stream.Run(ctx) }()
		defer func() {
			cancel()
			<-done
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				Fail("stream run: " + runErr.Error())
			}
		}()

		Eventually(func() int64 {
			return captureExporter.counterValue("dropped_spoofed_key")
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(Equal(int64(1)), "the spoofed-header drop must increment dropped_spoofed_key on the outer stream's metrics registry")

		// Spoof-first classification: the record must NOT be miscounted as keyless
		// (its post-header key is the shadowing value) or as a real filter drop.
		Expect(captureExporter.counterValue("dropped_keyless")).To(Equal(int64(0)),
			"a spoofed record must not count as keyless")
		Expect(captureExporter.counterValue("filtered_records")).To(Equal(int64(0)),
			"a spoofed record must not count as a filtered (real-keyed) drop")
		// Both records' NATIVE keys matched the pattern, so connect omitted
		// neither: the matching-native spoof is the residual vector uns_beta still
		// observes, not a connect-side omit. (The non-matching-native spoof-DELIVER
		// vector — closed at the source — would instead show up here as
		// key_omitted_records; see the Describe comment.)
		Expect(captureExporter.counterValue("key_omitted_records")).To(Equal(int64(0)),
			"both native keys matched the pattern, so connect omitted nothing")
		mu.Lock()
		Expect(delivered).To(Equal(1), "the keyed matching record must be delivered")
		mu.Unlock()
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
