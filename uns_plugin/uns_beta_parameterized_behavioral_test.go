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

// Specs for uns_beta's dropped_keyless and filtered_records counters, and the
// unsBetaInputYAML config helper.

package uns_plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	// Registers the "none" tracer that StreamBuilder.Build defaults to.
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

// unsBetaInputYAML builds a uns_beta input config with one umh_topics pattern.
func unsBetaInputYAML(broker, group, umhTopic string) string {
	return fmt.Sprintf(`
uns_beta:
  broker_address: %q
  consumer_group: %q
  umh_topics:
    - %q
`, broker, group, umhTopic)
}

var _ = Describe("uns_beta counters", Label("uns_beta"), func() {
	// Under `.*` connect passes a keyless record to ReadBatch, which drops it
	// and counts it once in dropped_keyless.
	It("counts a keyless record once in dropped_keyless", func() {
		registerCaptureExporter()
		captureExporter.reset()

		addr := startBroker(GinkgoT())
		group := fmt.Sprintf("uns-beta-param-keyless-%d", time.Now().UnixNano())
		produce(GinkgoT(), addr,
			&kgo.Record{Key: nil, Value: []byte(`{"v":0}`)},
			rec("umh.v1.keep.live", `{"v":1}`))

		var mu sync.Mutex
		var delivered int
		sb := service.NewStreamBuilder()
		Expect(sb.AddInputYAML(unsBetaInputYAML(addr, group, `.*`))).To(Succeed())
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
			Should(Equal(int64(1)), "uns_beta must increment dropped_keyless for a keyless record that reaches it under a match-everything pattern")

		// A redelivered and re-dropped keyless record would push the count
		// past 1.
		Consistently(func() int64 {
			return captureExporter.counterValue("dropped_keyless")
		}).WithTimeout(2*time.Second).WithPolling(200*time.Millisecond).
			Should(Equal(int64(1)), "dropped_keyless climbed past 1, so the keyless record was redelivered")

		Expect(captureExporter.counterValue("filtered_records")).To(Equal(int64(0)),
			"a keyless record must not count as a filtered (real-keyed) drop")
		// The keyed record can arrive in a later poll than the keyless one.
		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return delivered
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(Equal(1), "the keyed matching record must be delivered")
	})

	// Connect's key_pattern pre-filter matches the real record key, but
	// uns_beta matches the kafka_key metadata. A kafka_key header overrides
	// that metadata (ENG-5125), so only such a record reaches filteredCounter.
	It("counts a record whose kafka_key header fails the filter in filtered_records", func() {
		registerCaptureExporter()
		captureExporter.reset()

		addr := startBroker(GinkgoT())
		group := fmt.Sprintf("uns-beta-param-filtered-%d", time.Now().UnixNano())
		produce(GinkgoT(), addr, &kgo.Record{
			Key:     []byte("umh.v1.acme.real"),
			Value:   []byte(`{"v":1}`),
			Headers: []kgo.RecordHeader{{Key: "kafka_key", Value: []byte("umh.v1.other.spoofed")}},
		})

		sb := service.NewStreamBuilder()
		Expect(sb.AddInputYAML(unsBetaInputYAML(addr, group, `^umh\.v1\.acme\..+`))).To(Succeed())
		Expect(sb.SetMetricsYAML(captureExporterName + ": {}")).To(Succeed())
		Expect(sb.AddBatchConsumerFunc(func(_ context.Context, _ service.MessageBatch) error {
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
			return captureExporter.counterValue("filtered_records")
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).
			Should(Equal(int64(1)))
	})
})
