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
	"net"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	// Registers the "none" tracer that StreamBuilder.Build defaults to.
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

// runUnsBetaStream starts a StreamBuilder pipeline: uns_beta input -> consumerFn.
// consumerFn's returned error NACKs the batch. Returns a stop func (see
// startStream).
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
	return startStream(t, stream)
}

// startStream runs a built stream in the background and returns an idempotent
// stop func that stops the pipeline before the Run context is canceled.
func startStream(t testingT, stream *service.Stream) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error // written before close(done), read after <-done
	go func() { defer close(done); runErr = stream.Run(ctx) }()
	var stopOnce sync.Once
	return func() {
		// Specs call stop() both explicitly and via defer.
		stopOnce.Do(func() {
			// A bare cancel returns before the franz client stops, and the leaked
			// consumer keeps committing on the group. StopWithin waits for it, so
			// a restarted stream gets the group to itself.
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
		group := "uns-beta-delivery"
		produce(GinkgoT(), addr, rec("umh.v1.acme.berlin.temp", `{"v":1}`))

		var mu sync.Mutex
		var got []string
		stop := runUnsBetaStream(GinkgoT(), unsBetaInputYAML(addr, group, `.*`), func(_ context.Context, b service.MessageBatch) error {
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

		// A broken ack path still delivers, but replays the whole topic on
		// restart. The 15s timeout covers the 5s commit_period in
		// redpandaConfigYAML.
		stop()
		Eventually(func(g Gomega) {
			off, ok, err := committedOffsetE(addr, group)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "no offset committed yet")
			g.Expect(off).To(Equal(int64(1)))
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	})
})

// Every message carries umh_topic and kafka_msg_key as copies of the kafka_key
// metadata, next to the metadata the redpanda input sets (kafka_key,
// kafka_topic, kafka_timestamp_ms, headers). The copies override a umh_topic
// header sent by the producer.
var _ = Describe("uns_beta metadata contract", Label("uns_beta"), func() {
	It("aliases each message's own record key and passes the native redpanda metadata through", func() {
		addr := startBroker(GinkgoT())
		group := "uns-beta-meta"
		const key1 = "umh.v1.acme.k1"
		const key2 = "umh.v1.acme.k2"
		// Two distinct keys: copying the first key onto the whole batch would
		// silently route data to the wrong UNS topic.
		produce(GinkgoT(), addr,
			&kgo.Record{
				Key:   []byte(key1),
				Value: []byte(`{"v":1}`),
				Headers: []kgo.RecordHeader{
					{Key: "h-multi", Value: []byte("hello")},
					{Key: "umh_topic", Value: []byte("umh.v1.evil.divergent")},
				},
			},
			&kgo.Record{Key: []byte(key2), Value: []byte(`{"v":2}`)},
		)

		var mu sync.Mutex
		seen := map[string]map[string]string{} // payload -> metadata snapshot
		stop := runUnsBetaStream(GinkgoT(), unsBetaInputYAML(addr, group, `.*`), func(_ context.Context, b service.MessageBatch) error {
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
		// The uns output copies metadata into Kafka headers, so a surviving
		// header slice would grow with every hop.
		Expect(m1).NotTo(HaveKey(rpcnKafkaHeadersKey), rpcnKafkaHeadersKey+" must be stripped before delivery")

		Expect(m2["umh_topic"]).To(Equal(key2), "umh_topic must alias the record's own key")
		Expect(m2["kafka_msg_key"]).To(Equal(key2), "kafka_msg_key must alias the record's own key")
	})

	// The redpanda input stores each header value as a string, so a
	// single-byte header reads back as "x", not as the decimal of its byte
	// ("120"). uns_beta does not normalize headers itself, so this spec pins
	// connect's behaviour.
	It("reads single-byte, empty, and multi-byte headers back as strings without corrupting native int fields", func() {
		addr := startBroker(GinkgoT())
		group := "uns-beta-header-typing"
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
		stop := runUnsBetaStream(GinkgoT(), unsBetaInputYAML(addr, group, `.*`), func(_ context.Context, b service.MessageBatch) error {
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

		Expect(m["h-multi"]).To(Equal("hello"), "multi-byte header must read back as its string")
		Expect(m["h-single"]).To(Equal("x"), "single-byte header must read back as its string, not the decimal of its byte")
		Expect(m).To(HaveKey("h-empty"), "empty header must still appear in metadata")
		Expect(m["h-empty"]).To(Equal(""), "empty header must read back as the empty string")

		// Integer metadata stays a decimal string, not a string(rune).
		Expect(m["kafka_timestamp_ms"]).To(MatchRegexp(`^\d+$`), "kafka_timestamp_ms must stay a decimal string")
		Expect(m).To(HaveKey("kafka_partition"))
		Expect(m["kafka_partition"]).To(Equal("0"), "kafka_partition must stay its decimal string, not a control character")

		Expect(m["umh_topic"]).To(Equal(key), "umh_topic alias must equal the record key")
		Expect(m["kafka_msg_key"]).To(Equal(key), "kafka_msg_key alias must equal the record key")
	})

	// Known defect (ENG-5125): the redpanda input applies record headers after
	// its own metadata, so a producer header named "kafka_key" replaces the
	// record key before the umh_topics filter and the umh_topic copy. These
	// two specs pin the current behaviour; replace them when it is fixed.
	It("stamps a spoofed kafka_key header over the real record key (documented gap)", func() {
		addr := startBroker(GinkgoT())
		group := "uns-beta-spoof"
		produce(GinkgoT(), addr, &kgo.Record{
			Key:   []byte("umh.v1.acme.real"),
			Value: []byte(`{"v":1}`),
			Headers: []kgo.RecordHeader{
				{Key: "kafka_key", Value: []byte("umh.v1.spoofed")},
			},
		})

		var mu sync.Mutex
		var umhTopics []string
		stop := runUnsBetaStream(GinkgoT(), unsBetaInputYAML(addr, group, `.*`), func(_ context.Context, b service.MessageBatch) error {
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

	// ENG-5125, data loss: a record whose real key matches but whose
	// "kafka_key" header does not is dropped, and the shared ack commits past
	// it. The matching ride-along record keeps the batch from being
	// all-filtered.
	It("drops and commits past a matching record whose spoofed kafka_key header fails the filter (documented gap)", func() {
		addr := startBroker(GinkgoT())
		group := "uns-beta-spoof-dataloss"
		produce(GinkgoT(), addr,
			&kgo.Record{
				// offset 0: the real key matches, the spoofed header does not.
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
		stop := runUnsBetaStream(GinkgoT(), unsBetaInputYAML(addr, group, `umh\.v1\.acme\..+`), func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range b {
				bs, _ := m.AsBytes()
				got = append(got, string(bs))
			}
			return nil
		})
		defer stop()

		// Once the commit covers both records, a leaked delivery of the spoofed
		// record would already have happened.
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

// Only records whose Kafka key matches a umh_topics pattern are delivered, and
// a keyless record never matches (see betaKeyFilter.matches). The ack of the
// kept records also commits past the dropped ones.
// A tombstone is a record with a key and no value. Kafka uses it to mark a key
// deleted, and a compacted UNS topic carries them. uns_beta delivers it: the key
// matches, so the message reaches the pipeline with an empty body.
var _ = Describe("uns_beta tombstone", Label("uns_beta"), func() {
	It("delivers a tombstone as an empty message carrying its umh_topic", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-uat-tombstone"
		const key = "umh.v1.acme.berlin._historian.deleted"

		produce(GinkgoT(), addr, &kgo.Record{Key: []byte(key), Value: nil})

		type seen struct {
			body      string
			umhTopic  string
			tombstone string
		}
		got := make(chan seen, 4)

		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
  umh_topics:
    - "umh\\.v1\\.acme\\..+"
`, func(_ context.Context, batch service.MessageBatch) error {
			for _, msg := range batch {
				b, err := msg.AsBytes()
				if err != nil {
					return err
				}
				topic, _ := msg.MetaGet("umh_topic")
				ts, _ := msg.MetaGet("kafka_tombstone_message")
				select {
				case got <- seen{body: string(b), umhTopic: topic, tombstone: ts}:
				default:
				}
			}
			return nil
		})
		defer stop()

		var s seen
		Eventually(got).WithTimeout(20 * time.Second).Should(Receive(&s))
		Expect(s.umhTopic).To(Equal(key), "a tombstone must carry its key as umh_topic")
		Expect(s.body).To(BeEmpty(), "a tombstone has no value, so the message body is empty")
		Expect(s.tombstone).To(Equal("true"),
			"connect marks a tombstone with kafka_tombstone_message, which is how a pipeline can tell one from an empty payload")
	})
})

var _ = Describe("uns_beta umh_topics key filter", Label("uns_beta"), func() {
	It("delivers only matching keys, drops keyless records, and commits past the dropped offsets", func() {
		addr := startBroker(GinkgoT())
		group := "uns-beta-keyfilter"
		// One produce call, so under kfake the records almost always share one
		// batch. The matching record keeps it from being all-filtered.
		produce(GinkgoT(), addr,
			rec("umh.v1.acme.berlin.temp", `{"v":"berlin"}`), // offset 0: matches
			rec("umh.v1.acme.munich.temp", `{"v":"munich"}`), // offset 1: dropped, key does not match
			&kgo.Record{Value: []byte(`{"v":"keyless"}`)},    // offset 2: dropped, keyless never matches
		)

		var mu sync.Mutex
		var got []string
		stop := runUnsBetaStream(GinkgoT(), unsBetaInputYAML(addr, group, `umh\.v1\.acme\.berlin\..+`), func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range b {
				bs, _ := m.AsBytes()
				got = append(got, string(bs))
			}
			return nil
		})
		defer stop()

		// Once the commit covers all three records, a leaked delivery of a
		// dropped record would already have happened.
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

	// `.*` matches the empty string, yet a keyless record is still dropped.
	It("drops a keyless record under an explicit match-everything filter and still commits past it", func() {
		addr := startBroker(GinkgoT())
		group := "uns-beta-keyless"
		// The keyed record keeps the batch from being all-filtered.
		produce(GinkgoT(), addr,
			rec("umh.v1.acme.keyed", `{"v":1}`),   // offset 0: keyed, delivered
			&kgo.Record{Value: []byte(`{"v":2}`)}, // offset 1: keyless, dropped
		)

		var mu sync.Mutex
		var got []string
		stop := runUnsBetaStream(GinkgoT(), unsBetaInputYAML(addr, group, `.*`), func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range b {
				bs, _ := m.AsBytes()
				got = append(got, string(bs))
			}
			return nil
		})
		defer stop()

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

// Benthos discards an empty batch without calling its AckFunc. ReadBatch acks
// an all-filtered poll itself, so the offset advances and the partition does
// not stall.
// The pull request says "A keyless record with that header is delivered under
// the header's topic too." The UAT found that holds only under a pattern that
// matches the empty key: a narrow pattern drops the record in connect's
// pre-filter, which matches the raw key, before the header is ever read.
//
// These two specs are about the KEYLESS case, which no existing spec covers.
// The suite already pins the keyed case ("stamps a spoofed kafka_key header
// over the real record key", in this file) and keyless records
// without a header ("drops a keyless record under an explicit match-everything
// filter"). The gap is a keyless record that carries the header, which is the
// shape the pull request describes.
var _ = Describe("uns_beta keyless record carrying a kafka_key header", Label("uns_beta"), func() {
	It("drops it under a narrow pattern, because the pre-filter reads the raw key and never sees the header", func() {
		registerCaptureExporter()
		captureExporter.reset()

		addr := startBroker(GinkgoT())
		const group = "uns-beta-uat-keyless-header-narrow"

		produce(GinkgoT(), addr, &kgo.Record{
			Key:     nil,
			Value:   []byte(`{"v":0}`),
			Headers: []kgo.RecordHeader{{Key: "kafka_key", Value: []byte("umh.v1.acme.berlin._historian.spoofed")}},
		})

		delivered := make(chan string, 4)

		sb := service.NewStreamBuilder()
		Expect(sb.AddInputYAML(`
uns_beta:
  broker_address: "` + addr + `"
  consumer_group: "` + group + `"
  umh_topics:
    - "umh\\.v1\\.acme\\..+"
`)).To(Succeed())
		Expect(sb.SetMetricsYAML(captureExporterName + ": {}")).To(Succeed())
		Expect(sb.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
			for _, msg := range batch {
				t, _ := msg.MetaGet("umh_topic")
				select {
				case delivered <- t:
				default:
				}
			}
			return nil
		})).To(Succeed())
		stream, err := sb.Build()
		Expect(err).NotTo(HaveOccurred())
		defer startStream(GinkgoT(), stream)()

		Eventually(func() int64 { return captureExporter.counterValue("key_omitted_records") }).
			WithTimeout(20*time.Second).WithPolling(100*time.Millisecond).
			Should(Equal(int64(1)),
				"a keyless record must be omitted by the pre-filter under a narrow pattern, whatever its headers say")

		Consistently(delivered).WithTimeout(2*time.Second).ShouldNot(Receive(),
			"the record must not be delivered under the topic its kafka_key header claims")
	})

	It("delivers it under \".*\", under the topic the header claims, which is the ENG-5125 exposure", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-uat-keyless-header-wildcard"
		const spoofed = "umh.v1.acme.berlin._historian.spoofed"

		produce(GinkgoT(), addr, &kgo.Record{
			Key:     nil,
			Value:   []byte(`{"v":0}`),
			Headers: []kgo.RecordHeader{{Key: "kafka_key", Value: []byte(spoofed)}},
		})

		delivered := make(chan string, 4)
		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
  umh_topics:
    - ".*"
`, func(_ context.Context, batch service.MessageBatch) error {
			for _, msg := range batch {
				t, _ := msg.MetaGet("umh_topic")
				select {
				case delivered <- t:
				default:
				}
			}
			return nil
		})
		defer stop()

		var topic string
		Eventually(delivered).WithTimeout(20 * time.Second).Should(Receive(&topic))
		Expect(topic).To(Equal(spoofed),
			"this pins the known ENG-5125 exposure: the header overrides the record key, so the message is delivered, and would be written back, under a topic it does not belong to")
	})
})

// A key carrying a byte outside the UMH topic alphabet passes both of uns_beta's
// filters — the user's umh_topics regex is the only key validation it does — and
// is then refused by the uns output, which validates against the UMH topic
// grammar. Measured in a real UMH Core on 2026-11-26: the output rejected such a
// record about once a second indefinitely and nothing behind it on the partition
// was delivered.
//
// This spec pins the current behaviour so the gap is visible. Replace it when
// ENG-5125's parse-gate lands: at that point uns_beta should drop the record and
// count it, exactly as it already does for an empty key, and this spec should
// assert the drop instead of the delivery.
var _ = Describe("uns_beta key validation", Label("uns_beta"), func() {
	It("delivers a key the uns output cannot accept, which is what makes it a poison message (documented gap, ENG-5125)", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-uat-invalid-topic-key"

		// 0xff is not valid UTF-8 and is outside the UMH topic alphabet
		// [a-zA-Z0-9._-], which the uns output enforces.
		badKey := "umh.v1.acme.berlin._historian.bad\xffUTF"
		produce(GinkgoT(), addr, &kgo.Record{Key: []byte(badKey), Value: []byte(`{"v":1}`)})

		delivered := make(chan string, 4)
		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
  umh_topics:
    - "umh\\.v1\\.acme\\..+"
`, func(_ context.Context, batch service.MessageBatch) error {
			for _, msg := range batch {
				t, _ := msg.MetaGet("umh_topic")
				select {
				case delivered <- t:
				default:
				}
			}
			return nil
		})
		defer stop()

		var topic string
		Eventually(delivered).WithTimeout(20 * time.Second).Should(Receive(&topic))
		Expect(topic).To(Equal(badKey),
			"uns_beta validates the key only against umh_topics, so a key the uns output will always reject is delivered and stamped as umh_topic; this is what turns one malformed record from a third-party producer into a permanent stall of the partition")
		Expect(topic).To(ContainSubstring("\xff"),
			"the byte that the uns output refuses must still be present in the delivered topic — if it were sanitised here there would be no gap")
	})
})

var _ = Describe("uns_beta all-filtered poll commit", Label("uns_beta"), func() {
	It("self-acks an all-filtered poll so the committed offset advances while the consumer receives nothing", func() {
		addr := startBroker(GinkgoT())
		group := "uns-beta-all-filtered"
		// No key matches the filter below.
		produce(GinkgoT(), addr,
			rec("umh.v1.acme.berlin.temp", `{"v":0}`),
			rec("umh.v1.acme.munich.temp", `{"v":1}`),
			rec("umh.v1.acme.hamburg.temp", `{"v":2}`))

		var mu sync.Mutex
		var got []string
		stop := runUnsBetaStream(GinkgoT(), unsBetaInputYAML(addr, group, `^only-this$`), func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range b {
				bs, _ := m.AsBytes()
				got = append(got, string(bs))
			}
			return nil
		})
		defer stop()

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

	// Thousands of keys, one of which matches, so nearly every fetch is
	// all-filtered. The stream drains to the high-water mark.
	It("drains a high-cardinality selective stream to completion, delivering only the matching record", func() {
		addr := startBroker(GinkgoT())
		group := "uns-beta-selective-drain"
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
		stop := runUnsBetaStream(GinkgoT(), unsBetaInputYAML(addr, group, `^umh\.v1\.match\..+$`), func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range b {
				bs, _ := m.AsBytes()
				got = append(got, string(bs))
			}
			return nil
		})
		defer stop()

		// Draining 2000 records plus the 5s commit_period can take most of 15s
		// on a slow kfake start, hence 30s.
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

// A fresh consumer group starts at the earliest offset, so records produced
// before it started are delivered. uns_beta_translation_parity_test.go checks
// the start_offset value; this spec checks the behaviour.
var _ = Describe("uns_beta start-from-earliest", Label("uns_beta"), func() {
	It("delivers records produced before a fresh consumer group started", func() {
		addr := startBroker(GinkgoT())
		group := fmt.Sprintf("uns-beta-earliest-%d", time.Now().UnixNano())

		// Produce before any consumer exists.
		produce(GinkgoT(), addr,
			rec("umh.v1.enterprise.siteA._raw.temp", `{"v":"A"}`),
			rec("umh.v1.enterprise.siteB._raw.temp", `{"v":"B"}`))

		var mu sync.Mutex
		var got []string
		stop := runUnsBetaStream(GinkgoT(), unsBetaInputYAML(addr, group, `^umh\.v1\.`), func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range b {
				bs, _ := m.AsBytes()
				got = append(got, string(bs))
			}
			return nil
		})
		defer stop()

		Eventually(func() []string {
			mu.Lock()
			defer mu.Unlock()
			return append([]string(nil), got...)
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(ContainElements(`{"v":"A"}`, `{"v":"B"}`),
				"both records produced before the fresh consumer group started must be delivered (start_offset = earliest)")
	})
})

// A mixed poll whose kept record is NACKed for longer than the 5s
// commit_period: auto_replay_nacks replays the whole poll, ReadBatch drops the
// filtered record again, and no commit tick moves the offset past the NACKed
// record. Once the record is acked the offset commits past both.
var _ = Describe("uns_beta mixed-batch NACK redelivery", Label("uns_beta"), func() {
	It("does not commit past a NACKed kept record across commit ticks and re-drops the filtered one", func() {
		addr := startBroker(GinkgoT())
		group := "uns-beta-mixed-nack"
		const nackedOffset = 0
		produce(GinkgoT(), addr,
			rec("umh.v1.acme.keep", `{"v":"keep"}`), // offset 0: kept, NACKed
			rec("umh.v1.evil.drop", `{"v":"drop"}`), // offset 1: dropped by the filter
		)

		// nackFor must exceed the 5s commit_period, so at least one commit
		// tick fires while the record is NACKed.
		const nackFor = 8 * time.Second
		var mu sync.Mutex
		var firstNack time.Time
		acked := false

		stop := runUnsBetaStream(GinkgoT(), unsBetaInputYAML(addr, group, `^umh\.v1\.acme\..+$`), func(_ context.Context, b service.MessageBatch) error {
			defer GinkgoRecover()
			mu.Lock()
			defer mu.Unlock()
			Expect(b).To(HaveLen(1), "only the kept record may reach the consumer, even on replay")
			bs, _ := b[0].AsBytes()
			Expect(string(bs)).To(Equal(`{"v":"keep"}`), "the filtered record must never be delivered, even on replay")

			if firstNack.IsZero() {
				firstNack = time.Now()
			}
			if time.Since(firstNack) < nackFor {
				return errors.New("simulated output failure")
			}
			acked = true
			return nil
		})
		defer stop()

		Eventually(func() bool {
			mu.Lock()
			defer mu.Unlock()
			return !firstNack.IsZero()
		}).WithTimeout(15*time.Second).WithPolling(50*time.Millisecond).
			Should(BeTrue(), "the kept record never reached the consumer")

		committed := func() int64 {
			off, ok, err := committedOffsetE(addr, group)
			Expect(err).NotTo(HaveOccurred())
			if !ok {
				return 0
			}
			return off
		}
		Consistently(committed).WithTimeout(7*time.Second).WithPolling(250*time.Millisecond).
			Should(BeNumerically("<=", nackedOffset), "the offset was committed past the record while it was still being NACKed")

		mu.Lock()
		windowInsideNacks := time.Since(firstNack) < nackFor
		mu.Unlock()
		Expect(windowInsideNacks).To(BeTrue(), "the Consistently window ran past the NACK period, so it does not prove anything")

		Eventually(func() bool {
			mu.Lock()
			defer mu.Unlock()
			return acked
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(BeTrue(), "the NACKed kept record was never redelivered to success")

		Eventually(func(g Gomega) {
			off, ok, err := committedOffsetE(addr, group)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "offset never committed after the kept record succeeded")
			g.Expect(off).To(Equal(int64(2)), "the offset commits past both records once the kept record succeeds")
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	})
})

// A restarted stream on the same consumer group resumes from the committed
// offset, with no loss and no replay. The log holds one match at offset 0 and
// then 1500 non-matching records, which span whole fetches. Connect's
// key_pattern pre-filter omits them; the placeholder it keeps for each poll's
// last omitted record carries the commit to the high-water mark.
// A rejected message replays its whole batch, so a message the pipeline already
// accepted is delivered again on every replay. The UAT measured 67 repeat
// deliveries of each accepted sibling during a 67-second downstream outage. A
// consumer of uns_beta must therefore be idempotent, and this spec fails if the
// input ever stops replaying the accepted siblings — which is what a move to
// per-message acks would do, and is why it is pinned here.
var _ = Describe("uns_beta batch replay", Label("uns_beta"), func() {
	It("redelivers the messages a batch-mate's rejection replays, and loses none once the rejection stops", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-uat-partial-batch"

		// Five records in one fetch. The pipeline rejects the third.
		produce(GinkgoT(), addr,
			rec("umh.v1.acme.berlin._historian.a", `{"v":0}`),
			rec("umh.v1.acme.berlin._historian.b", `{"v":1}`),
			rec("umh.v1.acme.berlin._historian.c", `{"v":2}`),
			rec("umh.v1.acme.berlin._historian.d", `{"v":3}`),
			rec("umh.v1.acme.berlin._historian.e", `{"v":4}`))

		const poison = `{"v":2}`
		log := newDeliveryLog()
		var rejecting = true
		var mu sync.Mutex

		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
  umh_topics:
    - "umh\\.v1\\.acme\\..+"
`, func(_ context.Context, batch service.MessageBatch) error {
			mu.Lock()
			reject := rejecting
			mu.Unlock()
			for _, msg := range batch {
				b, err := msg.AsBytes()
				if err != nil {
					return err
				}
				log.add(string(b))
				if reject && string(b) == poison {
					return errPoison
				}
			}
			return nil
		})
		defer stop()

		// The accepted sibling is redelivered while its batch-mate is rejected.
		Eventually(func() int { return log.count(`{"v":0}`) }).
			WithTimeout(30*time.Second).WithPolling(100*time.Millisecond).
			Should(BeNumerically(">", 1),
				"a message the pipeline already accepted must be redelivered while a batch-mate is rejected; if this stops being true the input has moved to per-message acks and the idempotency contract in the docs has changed")

		// Nothing commits past the rejected record while it keeps failing.
		off, ok := committedOffset(GinkgoT(), addr, group)
		Expect(ok && off > 2).To(BeFalse(),
			"the committed offset must not move past the rejected record, got %d (committed=%v)", off, ok)

		// Once the pipeline accepts, every record arrives and the offset catches up.
		mu.Lock()
		rejecting = false
		mu.Unlock()

		Eventually(func() int { return log.distinct() }).
			WithTimeout(30*time.Second).WithPolling(100*time.Millisecond).
			Should(Equal(5), "every produced record must reach the pipeline once the rejection stops")

		Eventually(func() int64 {
			off, _, err := committedOffsetE(addr, group)
			if err != nil {
				return -1
			}
			return off
		}).WithTimeout(30*time.Second).WithPolling(200*time.Millisecond).
			Should(Equal(int64(5)), "the committed offset must reach the end of the log once every record is accepted")
	})
})

var _ = Describe("uns_beta gapless restart", Label("uns_beta"), func() {
	It("resumes a restarted consumer group from the committed offset with no loss and no replay across a long omitted run", func() {
		addr := startBroker(GinkgoT())
		group := "uns-beta-gapless-restart"
		const nonMatching = 1500 // a long omitted run, spanning many fetches
		const total = 1 + nonMatching

		records := make([]*kgo.Record, total)
		records[0] = rec("umh.v1.match.first", `{"v":"first"}`)
		for i := 1; i < total; i++ {
			records[i] = rec(fmt.Sprintf("umh.v1.nomatch.k%d", i), `{"v":"drop"}`)
		}
		produce(GinkgoT(), addr, records...)

		streamYAML := unsBetaInputYAML(addr, group, `^umh\.v1\.match\..+$`)

		// Run 1 drains the whole log.
		var mu sync.Mutex
		var got []string
		noPlaceholder := true // set false if any message arrives without a kafka_key
		stop := runUnsBetaStream(GinkgoT(), streamYAML, func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range b {
				bs, _ := m.AsBytes()
				got = append(got, string(bs))
				// The placeholder has no kafka_key and must never be delivered.
				if _, ok := m.MetaGet("kafka_key"); !ok {
					noPlaceholder = false
				}
			}
			return nil
		})
		defer stop()

		// 30s: draining 1500 records plus the 5s commit_period can take most of
		// 15s on a slow kfake start.
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

		// Restart: stop run 1, then produce a second match.
		stop()
		produce(GinkgoT(), addr, rec("umh.v1.match.second", `{"v":"second"}`)) // offset == total

		// Run 2 is a new stream on the same group.
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

		// A lost commit replays the first match; a commit past an unacked
		// record skips the new one.
		Eventually(func() []string {
			mu2.Lock()
			defer mu2.Unlock()
			return append([]string(nil), got2...)
		}).WithTimeout(30*time.Second).WithPolling(100*time.Millisecond).
			Should(ConsistOf(`{"v":"second"}`),
				"the restarted group must deliver only the record produced after the committed offset — no replay of the acked first match, no loss of the new one")

		Eventually(func(g Gomega) {
			off, ok, err := committedOffsetE(addr, group)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue())
			g.Expect(off).To(Equal(int64(total+1)), "committed offset must advance past the post-restart record")
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	})
})

// closedLocalPort returns a loopback address that nothing listens on, so a
// dial to it is refused.
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

// connectProbeTimeout bounds how long the unreachable-broker spec waits for
// Connect to fail.
const connectProbeTimeout = 10 * time.Second

// Connect against an unreachable broker returns an error instead of starting a
// stream that delivers nothing.
var _ = Describe("uns_beta Connect against an unreachable broker", Label("uns_beta"), func() {
	It("returns a Connect error within the connect-probe bound", func() {
		addr := closedLocalPort(GinkgoT())

		// newUnsBeta builds the redpanda reader without dialing; Connect dials.
		var input service.BatchInput
		inputYAML := fmt.Sprintf(`
broker_address: %q
consumer_group: "uns-beta-unreachable"
umh_topics: [".*"]
`, addr)
		pConf, err := unsBetaConfigSpec().ParseYAML(inputYAML, service.GlobalEnvironment())
		Expect(err).NotTo(HaveOccurred(), "the reader config must parse")
		input, err = newUnsBeta(pConf, service.MockResources())
		Expect(err).NotTo(HaveOccurred(), "the reader must construct against a (closed) broker")
		defer func() {
			// Close waits for the redpanda reader, which keeps redialing the
			// closed port. Bound it; this suite does not check for leaked
			// goroutines.
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer closeCancel()
			_ = input.Close(closeCtx)
		}()

		// Off the spec goroutine, so a hung Connect fails the spec instead of
		// blocking it.
		done := make(chan error, 1)
		go func() { done <- input.Connect(context.Background()) }()

		var connErr error
		Eventually(done, connectProbeTimeout+5*time.Second, 50*time.Millisecond).
			Should(Receive(&connErr), "Connect must return within the connect-probe bound, not hang")
		Expect(connErr).To(HaveOccurred(),
			"Connect against a closed port must surface a real connection error, not start a stream that delivers nothing")
	})
})

// capturingMetricsExporter records the name of every metric created on it and
// the sum of every counter's increments, so a spec can check both that a
// metric reached the stream's registry and a counter's value.
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

// accumulatingCounter adds every Incr to the exporter's per-name total.
type accumulatingCounter struct {
	exporter *capturingMetricsExporter
	name     string
}

func (a *accumulatingCounter) Incr(v int64)          { a.exporter.addCount(a.name, v) }
func (a *accumulatingCounter) IncrFloat64(v float64) { a.exporter.addCount(a.name, int64(v)) }

// noopMetric is the timer and gauge instance; specs only check their names.
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

// Connect's key_pattern pre-filter omits a record whose key fails the pattern
// before it reaches ReadBatch, and counts it in key_omitted_records. uns_beta's
// filtered_records counts only records that reach ReadBatch and fail its own
// filter, so it stays 0 here.
var _ = Describe("uns_beta filtered_records counter", Label("uns_beta"), func() {
	It("counts records omitted by the connect pre-filter under key_omitted_records, leaving uns_beta's filtered_records at 0", func() {
		registerCaptureExporter()
		captureExporter.reset()

		addr := startBroker(GinkgoT())
		const group = "uns-beta-filtered-counter"
		// No key matches the filter below.
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

		defer startStream(GinkgoT(), stream)()

		Eventually(func() int64 {
			return captureExporter.counterValue("key_omitted_records")
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(Equal(int64(wantOmitted)), "the connect pre-filter's key_omitted_records counter did not reach the omitted-record count on the outer stream's metrics registry — pre-filtered records are unobservable")

		Expect(captureExporter.counterValue("filtered_records")).To(Equal(int64(0)),
			"records omitted by the connect pre-filter must not reach uns_beta's filtered_records counter")
		// The placeholder for the poll's last omitted record reaches ReadBatch
		// without kafka_key and is dropped without being counted.
		Expect(captureExporter.counterValue("dropped_keyless")).To(Equal(int64(0)),
			"the connect high-water placeholder must not be miscounted as dropped_keyless")
	})

	// key_omitted_records counts each omitted record, not each batch or poll.
	It("counts only the omitted records (M) under key_omitted_records, delivers the N matching records, and leaves uns_beta's per-reason counters at 0", func() {
		registerCaptureExporter()
		captureExporter.reset()

		addr := startBroker(GinkgoT())
		const group = "uns-beta-mixed-counter"
		// The last record matches, so connect keeps no placeholder for this poll.
		produce(GinkgoT(), addr,
			rec("umh.v1.keep.alpha", `{"v":0}`),   // delivered (matches pattern)
			rec("umh.v1.drop.beta", `{"v":1}`),    // omitted in connect
			rec("umh.v1.drop.delta", `{"v":2}`),   // omitted in connect
			rec("umh.v1.drop.epsilon", `{"v":3}`), // omitted in connect
			rec("umh.v1.keep.gamma", `{"v":4}`))   // delivered (matches pattern)
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

		defer startStream(GinkgoT(), stream)()

		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return delivered
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(Equal(wantDelivered), "the matching records must be delivered")

		Eventually(func() int64 {
			return captureExporter.counterValue("key_omitted_records")
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(Equal(int64(wantOmitted)), "key_omitted_records must count only the M omitted records, not the whole batch")

		Expect(captureExporter.counterValue("filtered_records")).To(Equal(int64(0)),
			"records omitted by the connect pre-filter must not reach uns_beta's filtered_records counter")
		Expect(captureExporter.counterValue("dropped_keyless")).To(Equal(int64(0)),
			"no record reaching uns_beta was keyless")
	})
})

// TODO(ENG-5125): add a dropped_spoofed_key increment spec once spoof
// detection lands; the counter is not registered yet.

// The uns_beta counterpart of TestUNSInput_DoesNotCommitPastNACK: a NACKed
// message is redelivered until it succeeds, and the offset commits after.
var _ = Describe("uns_beta NACK redelivery capstone", Label("uns_beta"), func() {
	It("redelivers a NACKed message until success and commits the offset only after", func() {
		addr := startBroker(GinkgoT())
		group := "uns-beta-capstone"
		// A occupies offset 0, B offset 1 (produce is synchronous, in call order).
		produce(GinkgoT(), addr,
			rec("umh.v1.acme.poison", `{"v":"A"}`),
			rec("umh.v1.acme.ok", `{"v":"B"}`))

		const payloadA = `{"v":"A"}`
		const failuresWanted = 3
		var mu sync.Mutex
		deliveries := map[string]int{} // payload -> delivery count
		stop := runUnsBetaStream(GinkgoT(), unsBetaInputYAML(addr, group, `.*`), func(_ context.Context, b service.MessageBatch) error {
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
				return errors.New("simulated output failure")
			}
			return nil
		})
		defer stop()

		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return deliveries[payloadA]
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(BeNumerically(">=", failuresWanted+1), "A was never redelivered to success")

		Eventually(func(g Gomega) {
			off, ok, err := committedOffsetE(addr, group)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "no offset committed yet")
			g.Expect(off).To(Equal(int64(2)))
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	})
})
